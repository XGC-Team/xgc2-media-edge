package mediaedge

import (
	"bytes"
	"flag"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The pipeline tests run the test binary itself as the encoder pipeline and as
// its producer: a first argument of "fake-pipeline" or "fake-producer" makes
// TestMain act as that process instead of running tests.
func TestMain(m *testing.M) {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "fake-pipeline":
			os.Exit(runFakePipeline(os.Args[2:]))
		case "fake-producer":
			os.Exit(runFakeProducer(os.Args[2:]))
		case "fake-video":
			os.Exit(runFakeVideo(os.Args[2:]))
		case "pipeline-host":
			os.Exit(runPipelineHost(os.Args[2:]))
		}
	}
	os.Exit(m.Run())
}

// runFakePipeline stands in for the encoder: it sends RTP-like datagrams to the
// destination, writes JPEG snapshots and leaves on SIGINT.
func runFakePipeline(arguments []string) int {
	flags := flag.NewFlagSet("fake-pipeline", flag.ContinueOnError)
	rtp := flags.String("rtp", "", "host:port of the RTP destination")
	snapshots := flags.String("snapshots", "", "snapshot file name pattern")
	size := flags.String("size", "64x48", "snapshot size")
	snapshotEvery := flags.Duration("snapshot-every", 80*time.Millisecond, "snapshot period")
	slowWrite := flags.Bool("slow-write", false, "write each snapshot in two halves")
	stdin := flags.Bool("stdin", false, "read standard input and leave when it ends")
	ignoreInterrupt := flags.Bool("ignore-interrupt", false, "keep running on SIGINT")
	exitAfter := flags.Duration("exit-after", 0, "leave by itself after this long")
	exitCode := flags.Int("exit-code", 1, "status of -exit-after")
	message := flags.String("message", "", "text for standard error")
	pidFile := flags.String("pid-file", "", "file receiving the process ID")
	if err := flags.Parse(arguments); err != nil {
		return 2
	}
	if *message != "" {
		fmt.Fprintln(os.Stderr, *message)
	}
	if *pidFile != "" {
		_ = os.WriteFile(*pidFile, []byte(strconv.Itoa(os.Getpid())), 0o600)
	}
	interrupts := make(chan os.Signal, 1)
	signal.Notify(interrupts, os.Interrupt, syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		for range interrupts {
			if !*ignoreInterrupt {
				close(done)
				return
			}
		}
	}()
	var exit <-chan time.Time
	if *exitAfter > 0 {
		exit = time.After(*exitAfter)
	}
	inputEnded := make(chan struct{})
	if *stdin {
		go func() {
			_, _ = io.Copy(io.Discard, os.Stdin)
			close(inputEnded)
		}()
	}
	var connection net.Conn
	if *rtp != "" {
		var err error
		if connection, err = net.Dial("udp", *rtp); err != nil {
			fmt.Fprintln(os.Stderr, "dial:", err)
			return 1
		}
		defer connection.Close()
	}
	width, height := 64, 48
	_, _ = fmt.Sscanf(*size, "%dx%d", &width, &height)
	packets := time.NewTicker(20 * time.Millisecond)
	defer packets.Stop()
	frames := time.NewTicker(*snapshotEvery)
	defer frames.Stop()
	var index, sequence uint16
	for {
		select {
		case <-done:
			return 0
		case <-exit:
			fmt.Fprintln(os.Stderr, "fatal: the encoder gave up")
			return *exitCode
		case <-inputEnded:
			fmt.Fprintln(os.Stderr, "input ended")
			return 3
		case <-packets.C:
			if connection != nil {
				packet := make([]byte, 24)
				packet[0], packet[1] = 0x80, 96
				packet[2], packet[3] = byte(sequence>>8), byte(sequence)
				sequence++
				_, _ = connection.Write(packet)
			}
		case <-frames.C:
			if *snapshots != "" {
				writeFakeSnapshot(*snapshots, uint(index), width, height, *slowWrite)
				index++
			}
		}
	}
}

func fakeSnapshotJPEG(width, height int, shade uint8) []byte {
	picture := image.NewRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			picture.Set(x, y, color.RGBA{R: 200, G: 100, B: shade, A: 255})
		}
	}
	var encoded bytes.Buffer
	_ = jpeg.Encode(&encoded, picture, &jpeg.Options{Quality: 95})
	return encoded.Bytes()
}

func writeFakeSnapshot(pattern string, index uint, width, height int, slow bool) {
	data := fakeSnapshotJPEG(width, height, uint8(index))
	file, err := os.Create(fmt.Sprintf(pattern, index))
	if err != nil {
		return
	}
	defer file.Close()
	if !slow {
		_, _ = file.Write(data)
	} else {
		half := len(data) / 2
		_, _ = file.Write(data[:half])
		time.Sleep(40 * time.Millisecond)
		_, _ = file.Write(data[half:])
	}
	// Keep two files, like the snapshot branch of a template.
	if index >= 2 {
		_ = os.Remove(fmt.Sprintf(pattern, index-2))
	}
}

// runFakeProducer stands in for the process that feeds an encoder reading
// standard input: it writes frames until its reader goes away.
func runFakeProducer(arguments []string) int {
	flags := flag.NewFlagSet("fake-producer", flag.ContinueOnError)
	frameBytes := flags.Int("frame-bytes", 64*48*3, "bytes per frame")
	every := flags.Duration("every", 50*time.Millisecond, "frame period")
	pidFile := flags.String("pid-file", "", "file receiving the process ID")
	if err := flags.Parse(arguments); err != nil {
		return 2
	}
	signal.Ignore(syscall.SIGPIPE)
	if *pidFile != "" {
		_ = os.WriteFile(*pidFile, []byte(strconv.Itoa(os.Getpid())), 0o600)
	}
	interrupts := make(chan os.Signal, 1)
	signal.Notify(interrupts, os.Interrupt)
	frame := make([]byte, *frameBytes)
	ticker := time.NewTicker(*every)
	defer ticker.Stop()
	for {
		select {
		case <-interrupts:
			return 0
		case <-ticker.C:
			if _, err := os.Stdout.Write(frame); err != nil {
				fmt.Fprintln(os.Stderr, "reader gone:", err)
				return 0
			}
		}
	}
}

// runFakeVideo is a producer of moving BGR frames for the real encoder
// pipelines: what xgc2_ros_image_pipe writes for a bgr8 topic.
func runFakeVideo(arguments []string) int {
	flags := flag.NewFlagSet("fake-video", flag.ContinueOnError)
	width := flags.Int("width", 160, "frame width")
	height := flags.Int("height", 120, "frame height")
	fps := flags.Int("fps", 10, "frames per second")
	flags.IntVar(fps, "max-fps", 10, "frames per second for the ROS pipe stub")
	flags.String("topic", "", "ROS topic for the ROS pipe stub")
	format := flags.String("format", "bgr8", "pixel format for the ROS pipe stub")
	pidFile := flags.String("pid-file", "", "file receiving the process ID")
	if err := flags.Parse(arguments); err != nil {
		return 2
	}
	if *format != "bgr8" {
		fmt.Fprintln(os.Stderr, "the video stub requires bgr8")
		return 2
	}
	signal.Ignore(syscall.SIGPIPE)
	if *pidFile != "" {
		_ = os.WriteFile(*pidFile, []byte(strconv.Itoa(os.Getpid())), 0o600)
	}
	interrupts := make(chan os.Signal, 1)
	signal.Notify(interrupts, os.Interrupt)
	frame := make([]byte, *width**height*3)
	ticker := time.NewTicker(time.Second / time.Duration(*fps))
	defer ticker.Stop()
	for index := 0; ; index++ {
		for y := 0; y < *height; y++ {
			for x := 0; x < *width; x++ {
				offset := (y**width + x) * 3
				frame[offset], frame[offset+1], frame[offset+2] = byte(x+index*4), byte(y*2), byte(x+y+index)
			}
		}
		select {
		case <-interrupts:
			return 0
		case <-ticker.C:
			if _, err := os.Stdout.Write(frame); err != nil {
				return 0
			}
		}
	}
}

// runPipelineHost is a process that owns a pipeline the way media-edge does. A
// test kills it to prove that the pipeline does not outlive its owner.
func runPipelineHost(arguments []string) int {
	templates, runtimeDir, pidFile := arguments[0], arguments[1], arguments[2]
	config, err := (Config{
		PipelineTemplatesDir: templates, PipelineRuntimeDir: runtimeDir, ControlAddress: "127.0.0.1:0",
		Sources: []SourceConfig{pipelineTestSource("host", "127.0.0.1:5999", "fake", map[string]PipelineValue{"pidFile": PipelineValue(pidFile)}, nil)},
	}).normalized()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	control := newPipelineControl(config.Sources[0], io.Discard)
	if err := control.start(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Println("ready")
	select {}
}

// ---- helpers shared by the pipeline tests ----

type testTemplate struct {
	name       string
	input      string
	snapshot   bool
	parameters map[string]pipelineParameterDocument
	command    []string
	producer   []string
}

func writeTestTemplate(t testing.TB, directory string, template testTemplate) {
	t.Helper()
	command := append([]string{testBinary(t)}, template.command...)
	quoted := make([]string, len(command))
	for index, argument := range command {
		quoted[index] = strconv.Quote(argument)
	}
	parameters := "{}"
	if len(template.parameters) != 0 {
		var items []string
		for name, parameter := range template.parameters {
			item := fmt.Sprintf("%q:{", name)
			if parameter.Default != nil {
				item += fmt.Sprintf(`"default":%q`, string(*parameter.Default))
			}
			item += "}"
			items = append(items, item)
		}
		parameters = "{" + strings.Join(items, ",") + "}"
	}
	producer := ""
	if template.producer != nil {
		arguments := append([]string{testBinary(t)}, template.producer...)
		quotedProducer := make([]string, len(arguments))
		for index, argument := range arguments {
			quotedProducer[index] = strconv.Quote(argument)
		}
		producer = fmt.Sprintf(`,"producer":[%s]`, strings.Join(quotedProducer, ","))
	}
	document := fmt.Sprintf(`{"name":%q,"description":"test","input":%q,"snapshot":%t,"parameters":%s,"command":[%s]%s}`,
		template.name, template.input, template.snapshot, parameters, strings.Join(quoted, ","), producer)
	if err := os.WriteFile(filepath.Join(directory, template.name+".json"), []byte(document), 0o600); err != nil {
		t.Fatal(err)
	}
}

func testBinary(t testing.TB) string {
	t.Helper()
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func pipelineTestSource(id, rtpAddress, template string, parameters map[string]PipelineValue, producer []string) SourceConfig {
	return SourceConfig{
		ID: id, Kind: SourceKindPipeline, RTPListenAddress: rtpAddress,
		Width: 64, Height: 48, FPS: 10, FrameID: id + "_optical",
		Pipeline: &PipelineConfig{Template: template, Parameters: parameters, Producer: producer},
	}
}

func defaultValue(value string) *PipelineValue {
	converted := PipelineValue(value)
	return &converted
}
