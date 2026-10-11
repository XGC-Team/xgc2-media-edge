package mediaedge

import (
	"bytes"
	"context"
	"encoding/binary"
	"image/jpeg"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// These tests run the shipped templates through the real GStreamer where it is
// installed, with its software encoder. The Jetson and Rockchip templates need
// their hardware plugins and are only checked for their structure.

func requireGStreamer(t *testing.T, elements ...string) {
	t.Helper()
	if _, err := exec.LookPath("gst-launch-1.0"); err != nil {
		t.Skip("gst-launch-1.0 is not installed")
	}
	for _, element := range elements {
		if err := exec.Command("gst-inspect-1.0", "--exists", element).Run(); err != nil {
			t.Skipf("GStreamer element %s is not installed", element)
		}
	}
}

// rtpObserver parses what a pipeline sends to the port MediaMTX listens on.
type rtpObserver struct {
	connection *net.UDPConn
	mu         sync.Mutex
	packets    int
	payload    map[uint8]bool
	nalTypes   map[int]bool
	idrStamps  map[uint32]bool
	sequences  []uint16
	stamps     []uint32
}

func newRTPObserver(t *testing.T) *rtpObserver {
	t.Helper()
	connection, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	if err := connection.SetReadBuffer(4 << 20); err != nil {
		t.Log("receive buffer:", err)
	}
	observer := &rtpObserver{connection: connection, payload: map[uint8]bool{}, nalTypes: map[int]bool{}, idrStamps: map[uint32]bool{}}
	go func() {
		buffer := make([]byte, 2048)
		for {
			size, _, err := connection.ReadFromUDP(buffer)
			if err != nil {
				return
			}
			observer.parse(buffer[:size])
		}
	}()
	t.Cleanup(func() { connection.Close() })
	return observer
}

func (observer *rtpObserver) address() string { return observer.connection.LocalAddr().String() }

func (observer *rtpObserver) parse(packet []byte) {
	if len(packet) < 13 || packet[0]>>6 != 2 {
		return
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	observer.packets++
	observer.payload[packet[1]&0x7f] = true
	sequence := binary.BigEndian.Uint16(packet[2:4])
	stamp := binary.BigEndian.Uint32(packet[4:8])
	observer.sequences = append(observer.sequences, sequence)
	observer.stamps = append(observer.stamps, stamp)
	note := func(nal int) {
		observer.nalTypes[nal] = true
		if nal == 5 {
			observer.idrStamps[stamp] = true
		}
	}
	switch kind := int(packet[12] & 0x1f); kind {
	case 24: // STAP-A: sizes followed by units
		for rest := packet[13:]; len(rest) > 3; {
			size := int(binary.BigEndian.Uint16(rest))
			if size == 0 || 2+size > len(rest) {
				break
			}
			note(int(rest[2] & 0x1f))
			rest = rest[2+size:]
		}
	case 28: // FU-A: only the start fragment names the unit
		if len(packet) > 13 && packet[13]&0x80 != 0 {
			note(int(packet[13] & 0x1f))
		}
	default:
		note(kind)
	}
}

func (observer *rtpObserver) count() int {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	return observer.packets
}

func (observer *rtpObserver) waitFor(packets int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if observer.count() >= packets {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return observer.count() >= packets
}

// processesWithArgument lists the processes whose command line has the argument.
func processesWithArgument(argument string) []int {
	entries, _ := os.ReadDir("/proc")
	var found []int
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid == os.Getpid() {
			continue
		}
		cmdline, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "cmdline"))
		if err != nil {
			continue
		}
		for _, part := range bytes.Split(cmdline, []byte{0}) {
			if string(part) == argument && !processGone(pid) {
				found = append(found, pid)
				break
			}
		}
	}
	return found
}

func TestShippedX264TemplateStreamsH264AndSnapshotsThroughGStreamer(t *testing.T) {
	requireGStreamer(t, "fdsrc", "rawvideoparse", "videoconvert", "tee", "queue", "x264enc", "h264parse", "rtph264pay", "udpsink", "videorate", "jpegenc", "multifilesink")
	observer := newRTPObserver(t)
	templates, err := filepath.Abs(shippedTemplates)
	if err != nil {
		t.Fatal(err)
	}
	runtime := t.TempDir()
	producerPID := filepath.Join(runtime, "producer.pid")
	producer := []string{testBinary(t), "fake-video", "--width=160", "--height=120", "--fps=10", "--pid-file=" + producerPID}
	source := pipelineTestSource("cam", observer.address(), "x264-fdsrc", map[string]PipelineValue{"format": "bgr", "bitrateKbps": "800"}, producer)
	source.Width, source.Height = 160, 120
	config, err := (Config{
		ControlAddress: "127.0.0.1:0", PipelineTemplatesDir: templates, PipelineRuntimeDir: filepath.Join(runtime, "pipelines"),
		Sources: []SourceConfig{source},
	}).normalized()
	if err != nil {
		t.Fatal(err)
	}
	logs := &lockedBuffer{}
	control := newPipelineControl(config.Sources[0], logs)
	t.Cleanup(control.close)
	if err := control.start(); err != nil {
		t.Fatalf("start: %v\n%s", err, logs.String())
	}
	_, port, _ := net.SplitHostPort(observer.address())
	if len(processesWithArgument("port="+port)) != 1 {
		t.Fatalf("expected one encoder process for port %s", port)
	}
	if !observer.waitFor(60, 10*time.Second) {
		t.Fatalf("only %d RTP packets arrived\n%s", observer.count(), logs.String())
	}
	time.Sleep(3 * time.Second)

	observer.mu.Lock()
	for _, nal := range []int{7, 8, 5} {
		if !observer.nalTypes[nal] {
			t.Errorf("the stream has no NAL unit of type %d (seen %v)", nal, observer.nalTypes)
		}
	}
	if len(observer.payload) != 1 || !observer.payload[96] {
		t.Errorf("payload types = %v, want only 96", observer.payload)
	}
	// One IDR per second at 10 frames per second, over about four seconds.
	if len(observer.idrStamps) < 3 {
		t.Errorf("%d IDR frames in about four seconds, want one per second", len(observer.idrStamps))
	}
	for index := 1; index < len(observer.sequences); index++ {
		if observer.sequences[index] != observer.sequences[index-1]+1 {
			t.Errorf("RTP sequence jumps from %d to %d", observer.sequences[index-1], observer.sequences[index])
			break
		}
		if int32(observer.stamps[index]-observer.stamps[index-1]) < 0 {
			t.Errorf("RTP timestamps go backwards")
			break
		}
	}
	observer.mu.Unlock()

	// The snapshot branch: a fresh complete JPEG of the source's size.
	yes := true
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	response, data, rgb, err := control.call(ctx, sourceControlRequest{Operation: "capture", SnapshotID: "s", RequireFresh: &yes})
	if err != nil {
		t.Fatalf("capture: %v\n%s", err, logs.String())
	}
	if response.Width != 160 || response.Height != 120 || len(rgb) != 160*120*3 {
		t.Fatalf("capture = %+v rgb=%d", response, len(rgb))
	}
	if picture, err := jpeg.Decode(bytes.NewReader(data)); err != nil || picture.Bounds().Dx() != 160 {
		t.Fatalf("snapshot JPEG: %v", err)
	}

	// Stopping ends the encoder and the producer.
	feeder := readPID(t, producerPID)
	control.stop()
	if !processGone(feeder) || len(processesWithArgument("port="+port)) != 0 {
		t.Fatal("stop left a process of the pipeline behind")
	}
}

func TestShippedX264DeviceTemplatesAreValidGStreamerPipelines(t *testing.T) {
	requireGStreamer(t, "v4l2src", "jpegdec", "videoconvert", "tee", "x264enc", "rtph264pay", "udpsink", "jpegenc", "multifilesink")
	for _, name := range []string{"x264-v4l2src-raw", "x264-v4l2src-mjpeg"} {
		t.Run(name, func(t *testing.T) {
			source, err := resolveShipped(t, shippedSource(name, map[string]PipelineValue{"device": "/dev/xgc-no-such-camera"}, nil))
			if err != nil {
				t.Fatal(err)
			}
			command := source.Pipeline.resolved.command
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			output, err := exec.CommandContext(ctx, command[0], command[1:]...).CombinedOutput()
			text := string(output)
			if err == nil {
				t.Fatal("a pipeline on a missing device ran")
			}
			// GStreamer parses the whole description before it opens the device,
			// so a typo in an element, property or caps is reported differently.
			for _, parseFailure := range []string{"no element", "no property", "syntax error", "erroneous pipeline", "could not link"} {
				if strings.Contains(text, parseFailure) {
					t.Fatalf("GStreamer rejects the template: %s", text)
				}
			}
			if !strings.Contains(text, "Cannot identify device '/dev/xgc-no-such-camera'") {
				t.Fatalf("the pipeline did not fail on the device: %s", text)
			}
		})
	}
}
