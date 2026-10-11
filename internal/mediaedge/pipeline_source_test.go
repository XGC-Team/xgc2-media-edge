package mediaedge

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"image/jpeg"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

type lockedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (buffer *lockedBuffer) Write(data []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.buffer.Write(data)
}

func (buffer *lockedBuffer) String() string {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.buffer.String()
}

// udpSink plays the part of MediaMTX's RTP listener.
type udpSink struct {
	connection *net.UDPConn
	received   atomic.Int64
}

func newUDPSink(t testing.TB) *udpSink {
	t.Helper()
	connection, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	sink := &udpSink{connection: connection}
	go func() {
		buffer := make([]byte, 2048)
		for {
			if _, _, err := connection.ReadFromUDP(buffer); err != nil {
				return
			}
			sink.received.Add(1)
		}
	}()
	t.Cleanup(func() { connection.Close() })
	return sink
}

func (sink *udpSink) address() string { return sink.connection.LocalAddr().String() }

func (sink *udpSink) waitFor(count int64, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if sink.received.Load() >= count {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return sink.received.Load() >= count
}

// processGone reports whether pid is no more, a zombie included: a process
// whose parent is gone may wait for an init that does not reap.
func processGone(pid int) bool {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return true
	}
	closing := bytes.LastIndexByte(data, ')')
	return closing < 0 || len(data) < closing+3 || data[closing+2] == 'Z' || data[closing+2] == 'X'
}

func waitProcessGone(t testing.TB, pid int, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if processGone(pid) {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return processGone(pid)
}

func readPID(t testing.TB, path string) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(path); err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil {
				return pid
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s was not written", path)
	return 0
}

// waitPIDChange waits until the process behind a pid file is another one.
func waitPIDChange(t testing.TB, path string, old int) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(path); err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil && pid != old {
				return pid
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s still names process %d", path, old)
	return 0
}

type pipelineFixture struct {
	t         *testing.T
	templates string
	runtime   string
	pidFile   string
	sink      *udpSink
	config    Config
	control   *pipelineControl
	logs      *lockedBuffer
}

// newPipelineFixture runs the fake pipeline with extra as its arguments. The
// fake sends RTP to the sink, and, if the template has a snapshot branch, JPEGs
// to the scratch directory.
func newPipelineFixture(t *testing.T, snapshot bool, extra ...string) *pipelineFixture {
	t.Helper()
	fixture := &pipelineFixture{
		t: t, templates: t.TempDir(), runtime: t.TempDir(), sink: newUDPSink(t), logs: &lockedBuffer{},
	}
	fixture.pidFile = filepath.Join(fixture.runtime, "encoder.pid")
	command := []string{"fake-pipeline", "--rtp=${rtpHost}:${rtpPort}", "--pid-file=" + fixture.pidFile}
	if snapshot {
		command = append(command, "--snapshots=${snapshotDir}/"+snapshotFilePattern)
	}
	writeTestTemplate(t, fixture.templates, testTemplate{name: "fake", input: "device", snapshot: snapshot, command: append(command, extra...)})
	fixture.build(pipelineTestSource("cam", fixture.sink.address(), "fake", nil, nil))
	return fixture
}

func (fixture *pipelineFixture) build(source SourceConfig) {
	fixture.t.Helper()
	config, err := (Config{
		ControlAddress: "127.0.0.1:0", PipelineTemplatesDir: fixture.templates, PipelineRuntimeDir: filepath.Join(fixture.runtime, "pipelines"),
		Sources: []SourceConfig{source},
	}).normalized()
	if err != nil {
		fixture.t.Fatal(err)
	}
	fixture.config = config
	fixture.control = newPipelineControl(config.Sources[0], fixture.logs)
	fixture.control.settle = 100 * time.Millisecond
	fixture.control.stopGrace = 500 * time.Millisecond
	fixture.t.Cleanup(fixture.control.close)
}

func (fixture *pipelineFixture) call(operation string) (sourceControlResponse, error) {
	fixture.t.Helper()
	response, _, _, err := fixture.control.call(context.Background(), sourceControlRequest{Operation: operation})
	return response, err
}

func (fixture *pipelineFixture) mustCall(operation string) sourceControlResponse {
	fixture.t.Helper()
	response, err := fixture.call(operation)
	if err != nil {
		fixture.t.Fatalf("%s: %v", operation, err)
	}
	return response
}

func (fixture *pipelineFixture) waitState(state string) sourceControlResponse {
	fixture.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if status := fixture.control.status(); status.State == state {
			return status
		}
		time.Sleep(10 * time.Millisecond)
	}
	fixture.t.Fatalf("pipeline never reached %q; status %+v logs %q", state, fixture.control.status(), fixture.logs.String())
	return sourceControlResponse{}
}

func TestPipelineDescribesItselfAsACameraSource(t *testing.T) {
	fixture := newPipelineFixture(t, true)
	description := fixture.mustCall("describe")
	_, port, _ := net.SplitHostPort(fixture.sink.address())
	if description.ProtocolVersion != 1 || description.SourceID != "cam" || description.Codec != "H264" ||
		description.RTPPayloadType != 96 || description.RTPClockRate != 90000 ||
		description.RTPHost != "127.0.0.1" || strconv.Itoa(description.RTPPort) != port ||
		description.Width != 64 || description.Height != 48 || description.FPS != 10 || description.FrameID != "cam_optical" ||
		!description.KeyframeRequestSupported || description.KeyframePolicy != "restart" {
		t.Fatalf("describe = %+v", description)
	}
	if got := strings.Join(description.Capabilities, ","); got != "start,stop,request-keyframe,capture,fresh-snapshot" {
		t.Fatalf("capabilities = %s", got)
	}
	// media-edge's own validation of every descriptor accepts it.
	described, err := describeSourceWith(context.Background(), fixture.config.Sources[0], fixture.control)
	if err != nil || !described.CaptureSupported || !described.KeyframeRequestSupported {
		t.Fatalf("descriptor validation: %+v %v", described, err)
	}
	withoutSnapshots := newPipelineFixture(t, false)
	described, err = describeSourceWith(context.Background(), withoutSnapshots.config.Sources[0], withoutSnapshots.control)
	if err != nil || described.CaptureSupported {
		t.Fatalf("pipeline without snapshot branch: %+v %v", described, err)
	}
	if reference, bound := fixture.control.boundReference(); bound || reference.Service != "" {
		t.Fatal("a pipeline has no service reference")
	}
	if fixture.control.boundInstance() == "" || fixture.control.boundInstance() == withoutSnapshots.control.boundInstance() {
		t.Fatal("pipeline controls must have distinct incarnations")
	}
	for _, operation := range []string{"configure", "unknown"} {
		if _, err := fixture.call(operation); err == nil {
			t.Errorf("%s was accepted", operation)
		}
	}
	configuration := fixture.mustCall("config")
	if configuration.Persistence != "ephemeral" || configuration.Applied.RTPPort != description.RTPPort {
		t.Fatalf("config = %+v", configuration)
	}
}

func TestPipelineRunsOnlyBetweenStartAndStop(t *testing.T) {
	fixture := newPipelineFixture(t, true)
	if status := fixture.mustCall("status"); status.State != "idle" || status.DesiredActive || status.AppliedActive || status.ManagedSourceID != "cam" {
		t.Fatalf("initial status = %+v", status)
	}
	receipt := fixture.mustCall("start")
	if receipt.State != "active" || !receipt.Active || receipt.Completion != "applied" || receipt.ManagedSourceID != "cam" || receipt.ConfigurationRevision == 0 {
		t.Fatalf("start receipt = %+v", receipt)
	}
	pid := readPID(t, fixture.pidFile)
	if !fixture.sink.waitFor(5, 3*time.Second) {
		t.Fatalf("no RTP arrived; logs %q", fixture.logs.String())
	}
	if status := fixture.mustCall("status"); status.State != "active" || !status.DesiredActive || !status.AppliedActive {
		t.Fatalf("running status = %+v", status)
	}
	// start is idempotent: it adopts the running pipeline.
	fixture.mustCall("start")
	if again := readPID(t, fixture.pidFile); again != pid {
		t.Fatalf("a second start spawned pid %d next to %d", again, pid)
	}
	stopped := fixture.mustCall("stop")
	if stopped.State != "idle" || stopped.Active || stopped.Completion != "applied" {
		t.Fatalf("stop receipt = %+v", stopped)
	}
	if !processGone(pid) {
		t.Fatal("the pipeline outlived stop")
	}
	time.Sleep(100 * time.Millisecond)
	settled := fixture.sink.received.Load()
	time.Sleep(200 * time.Millisecond)
	if fixture.sink.received.Load() != settled {
		t.Fatal("RTP kept arriving after stop")
	}
	if entries, _ := os.ReadDir(fixture.control.spec.snapshotDir); len(entries) != 0 {
		t.Fatalf("stop left %d snapshot files", len(entries))
	}
	if status := fixture.mustCall("stop"); status.State != "idle" {
		t.Fatalf("second stop = %+v", status)
	}
	fixture.mustCall("start")
	waitPIDChange(t, fixture.pidFile, pid)
}

func TestPipelineThatCannotStartReportsWhy(t *testing.T) {
	fixture := newPipelineFixture(t, false, "--exit-after=1ms", "--exit-code=7", "--message=cannot open /dev/video8")
	fixture.control.settle = 5 * time.Second
	_, err := fixture.call("start")
	if err == nil || !strings.Contains(err.Error(), "exit status 7") || !strings.Contains(err.Error(), "cannot open /dev/video8") {
		t.Fatalf("start error = %v", err)
	}
	status := fixture.mustCall("status")
	if status.State != "faulted" || !status.DesiredActive || status.AppliedActive || !strings.Contains(status.LastError, "the encoder gave up") {
		t.Fatalf("status = %+v", status)
	}
	if !strings.Contains(fixture.logs.String(), "pipeline cam encoder: cannot open /dev/video8") {
		t.Fatalf("the child's output was not logged: %q", fixture.logs.String())
	}
	// Stopping a pipeline that never ran leaves the source idle.
	if stopped := fixture.mustCall("stop"); stopped.State != "idle" {
		t.Fatalf("stop after failed start = %+v", stopped)
	}
	if status := fixture.control.status(); status.State != "idle" || status.LastError != "" || status.DesiredActive {
		t.Fatalf("status after stop = %+v", status)
	}
}

func TestPipelineThatCrashesIsFaultedUntilStartedAgain(t *testing.T) {
	fixture := newPipelineFixture(t, false, "--exit-after=600ms")
	fixture.mustCall("start")
	first := readPID(t, fixture.pidFile)
	status := fixture.waitState("faulted")
	if !status.DesiredActive || status.AppliedActive || !strings.Contains(status.LastError, "pipeline encoder exited: exit status 1") {
		t.Fatalf("status = %+v", status)
	}
	if !processGone(first) {
		t.Fatal("the crashed pipeline is still there")
	}
	fixture.mustCall("start")
	waitPIDChange(t, fixture.pidFile, first)
}

func TestKeyframeRequestRestartsOnlyAPipelineOlderThanOneGOP(t *testing.T) {
	fixture := newPipelineFixture(t, false)
	fixture.control.keyframeRestartAge = 400 * time.Millisecond
	if _, err := fixture.call("request-keyframe"); err == nil {
		t.Fatal("a keyframe was requested from a pipeline that is not running")
	}
	fixture.mustCall("start")
	pid := readPID(t, fixture.pidFile)
	if receipt := fixture.mustCall("request-keyframe"); receipt.Completion != "coalesced" || receipt.State != "active" {
		t.Fatalf("early request = %+v", receipt)
	}
	if readPID(t, fixture.pidFile) != pid {
		t.Fatal("a young pipeline was restarted")
	}
	time.Sleep(500 * time.Millisecond)
	receipt := fixture.mustCall("request-keyframe")
	if receipt.Completion != "restarted" || receipt.State != "active" || !receipt.Active {
		t.Fatalf("late request = %+v", receipt)
	}
	restarted := waitPIDChange(t, fixture.pidFile, pid)
	if !processGone(pid) {
		t.Fatalf("restart kept the old process %d next to %d", pid, restarted)
	}
	if !fixture.sink.waitFor(1, 2*time.Second) {
		t.Fatal("the restarted pipeline sends no RTP")
	}
}

func TestStopKillsAPipelineThatIgnoresInterrupt(t *testing.T) {
	fixture := newPipelineFixture(t, false, "--ignore-interrupt")
	fixture.mustCall("start")
	pid := readPID(t, fixture.pidFile)
	started := time.Now()
	fixture.mustCall("stop")
	if elapsed := time.Since(started); elapsed < fixture.control.stopGrace {
		t.Fatalf("stop returned after %s, before the grace of %s", elapsed, fixture.control.stopGrace)
	}
	if !processGone(pid) {
		t.Fatal("the stubborn pipeline survived stop")
	}
	if status := fixture.control.status(); status.State != "idle" {
		t.Fatalf("status = %+v", status)
	}
}

func TestEitherEndOfAProducerPipeEndsThePipeline(t *testing.T) {
	templates, runtime := t.TempDir(), t.TempDir()
	sink := newUDPSink(t)
	encoderPID, producerPID := filepath.Join(runtime, "encoder.pid"), filepath.Join(runtime, "producer.pid")
	writeTestTemplate(t, templates, testTemplate{
		name: "fake", input: "stdin", command: []string{"fake-pipeline", "--stdin", "--rtp=${rtpHost}:${rtpPort}", "--pid-file=" + encoderPID},
	})
	producer := []string{testBinary(t), "fake-producer", "--pid-file=" + producerPID, "--every=10ms"}
	for name, victim := range map[string]string{"producer": producerPID, "encoder": encoderPID} {
		t.Run(name+" dies", func(t *testing.T) {
			_ = os.Remove(encoderPID)
			_ = os.Remove(producerPID)
			config, err := (Config{
				ControlAddress: "127.0.0.1:0", PipelineTemplatesDir: templates, PipelineRuntimeDir: filepath.Join(runtime, "pipelines"),
				Sources: []SourceConfig{pipelineTestSource("cam", sink.address(), "fake", nil, producer)},
			}).normalized()
			if err != nil {
				t.Fatal(err)
			}
			logs := &lockedBuffer{}
			control := newPipelineControl(config.Sources[0], logs)
			control.settle = 100 * time.Millisecond
			control.stopGrace = 500 * time.Millisecond
			t.Cleanup(control.close)
			if err := control.start(); err != nil {
				t.Fatal(err)
			}
			encoder, feeder := readPID(t, encoderPID), readPID(t, producerPID)
			before := sink.received.Load()
			if !sink.waitFor(before+3, 3*time.Second) {
				t.Fatalf("no RTP arrived: %q", logs.String())
			}
			victimPID := readPID(t, victim)
			if err := syscall.Kill(victimPID, syscall.SIGKILL); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(5 * time.Second)
			for control.status().State != "faulted" && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			status := control.status()
			if status.State != "faulted" || !strings.Contains(status.LastError, name+" exited: signal: killed") {
				t.Fatalf("status = %+v logs %q", status, logs.String())
			}
			if !waitProcessGone(t, encoder, 3*time.Second) || !waitProcessGone(t, feeder, 3*time.Second) {
				t.Fatalf("a process of the pipeline survived the death of its %s", name)
			}
		})
	}
}

func TestPipelineProcessesDieWithTheirOwner(t *testing.T) {
	templates, runtime := t.TempDir(), t.TempDir()
	pidFile := filepath.Join(runtime, "encoder.pid")
	writeTestTemplate(t, templates, testTemplate{
		name: "fake", input: "device", snapshot: false,
		parameters: map[string]pipelineParameterDocument{"pidFile": {}},
		command:    []string{"fake-pipeline", "--pid-file=${pidFile}"},
	})
	host := exec.Command(testBinary(t), "pipeline-host", templates, filepath.Join(runtime, "pipelines"), pidFile)
	output, err := host.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	host.Stderr = os.Stderr
	if err := host.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = host.Process.Kill(); _ = host.Wait() }()
	ready := make(chan string, 1)
	go func() {
		line, _ := bufio.NewReader(output).ReadString('\n')
		ready <- strings.TrimSpace(line)
	}()
	select {
	case line := <-ready:
		if line != "ready" {
			t.Fatalf("host said %q", line)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the pipeline host did not start its pipeline")
	}
	pid := readPID(t, pidFile)
	if processGone(pid) {
		t.Fatal("the pipeline is not running")
	}
	if err := host.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = host.Wait()
	if !waitProcessGone(t, pid, 5*time.Second) {
		_ = syscall.Kill(pid, syscall.SIGKILL)
		t.Fatal("the pipeline outlived the process that owned it")
	}
}

func TestPipelineScratchDirectoryMustBeOwnedAndPrivate(t *testing.T) {
	fixture := newPipelineFixture(t, true)
	scratch := fixture.control.spec.snapshotDir
	if err := os.MkdirAll(scratch, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(scratch, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.call("start"); err == nil || !strings.Contains(err.Error(), "mode 0700") {
		t.Fatalf("start with a world readable scratch directory: %v", err)
	}
	if err := os.Chmod(scratch, 0o700); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(scratch, "snapshot-00099.jpg")
	if err := os.WriteFile(stale, fakeSnapshotJPEG(64, 48, 1), 0o600); err != nil {
		t.Fatal(err)
	}
	fixture.mustCall("start")
	if _, err := os.Stat(stale); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("a frame of an earlier run survived the start")
	}
}

func TestClosingAPipelineStopsItAndRemovesItsScratchDirectory(t *testing.T) {
	fixture := newPipelineFixture(t, true)
	fixture.mustCall("start")
	pid := readPID(t, fixture.pidFile)
	scratch := fixture.control.spec.snapshotDir
	if _, err := os.Stat(scratch); err != nil {
		t.Fatal(err)
	}
	fixture.control.close()
	if !processGone(pid) {
		t.Fatal("close left the pipeline running")
	}
	if _, err := os.Stat(scratch); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("scratch directory after close: %v", err)
	}
	if _, err := fixture.call("start"); err == nil {
		t.Fatal("a closed pipeline was started")
	}
	fixture.control.close() // closing twice is allowed
}

func (fixture *pipelineFixture) capture(request sourceControlRequest) (sourceControlResponse, []byte, []byte, error) {
	fixture.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request.Operation = "capture"
	return fixture.control.call(ctx, request)
}

func TestCaptureReturnsTheSnapshotBranchFrame(t *testing.T) {
	fixture := newPipelineFixture(t, true, "--snapshot-every=40ms")
	if _, _, _, err := fixture.capture(sourceControlRequest{}); err == nil || !strings.Contains(err.Error(), "not running") {
		t.Fatalf("capture without a running pipeline: %v", err)
	}
	fixture.mustCall("start")
	response, jpegBytes, rgb, err := fixture.capture(sourceControlRequest{SnapshotID: "snap-1"})
	if err != nil {
		t.Fatal(err)
	}
	if !response.OK || response.SourceID != "cam" || response.SnapshotID != "snap-1" || response.FrameID != "cam_optical" ||
		response.Width != 64 || response.Height != 48 || response.PixelFormat != "rgb8" ||
		response.JPEGBytes != len(jpegBytes) || response.RGBBytes != 64*48*3 || len(rgb) != 64*48*3 ||
		response.Sequence>>32 != 1 || response.Sequence&0xffffffff == 0 ||
		response.TimestampClockDomain != "system_realtime" || response.TimestampNanoseconds <= 0 ||
		len(response.CameraMatrix) != 0 {
		t.Fatalf("capture = %+v", response)
	}
	if _, err := jpeg.Decode(bytes.NewReader(jpegBytes)); err != nil {
		t.Fatalf("returned JPEG: %v", err)
	}
	// The solid test frame is (200, 100, shade): the RGB is the decoded JPEG.
	for _, pixel := range []int{0, 64*48/2 + 32, 64*48 - 1} {
		r, g := int(rgb[pixel*3]), int(rgb[pixel*3+1])
		if r < 190 || r > 210 || g < 90 || g > 110 {
			t.Fatalf("pixel %d decoded to %d,%d,%d", pixel, rgb[pixel*3], g, rgb[pixel*3+2])
		}
	}
	// JPEG only.
	no := false
	response, jpegBytes, rgb, err = fixture.capture(sourceControlRequest{IncludeRGB: &no})
	if err != nil || len(rgb) != 0 || response.RGBBytes != 0 || len(jpegBytes) == 0 {
		t.Fatalf("JPEG only capture: %+v rgb=%d %v", response, len(rgb), err)
	}
	// A fresh capture is a frame completed after the request, and so newer.
	yes := true
	first, _, _, err := fixture.capture(sourceControlRequest{RequireFresh: &yes, IncludeRGB: &no})
	if err != nil {
		t.Fatal(err)
	}
	second, _, _, err := fixture.capture(sourceControlRequest{RequireFresh: &yes, IncludeRGB: &no})
	if err != nil {
		t.Fatal(err)
	}
	if second.Sequence <= first.Sequence || second.TimestampNanoseconds <= first.TimestampNanoseconds {
		t.Fatalf("fresh captures did not advance: %d@%d then %d@%d", first.Sequence, first.TimestampNanoseconds, second.Sequence, second.TimestampNanoseconds)
	}
}

func TestCaptureNeverReturnsAFileThatIsStillBeingWritten(t *testing.T) {
	fixture := newPipelineFixture(t, true, "--snapshot-every=25ms", "--slow-write")
	fixture.mustCall("start")
	no := false
	for attempt := 0; attempt < 40; attempt++ {
		_, data, _, err := fixture.capture(sourceControlRequest{IncludeRGB: &no})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := jpeg.Decode(bytes.NewReader(data)); err != nil {
			t.Fatalf("capture %d returned a truncated JPEG: %v", attempt, err)
		}
		time.Sleep(7 * time.Millisecond)
	}
}

func TestCaptureNeedsASnapshotBranchOfTheRightSize(t *testing.T) {
	without := newPipelineFixture(t, false)
	without.mustCall("start")
	if _, _, _, err := without.capture(sourceControlRequest{}); !errors.Is(err, ErrCaptureUnsupported) {
		t.Fatalf("capture without snapshot branch: %v", err)
	}
	wrongSize := newPipelineFixture(t, true, "--size=32x32", "--snapshot-every=20ms")
	wrongSize.mustCall("start")
	if _, _, _, err := wrongSize.capture(sourceControlRequest{}); err == nil || !strings.Contains(err.Error(), "is 32x32, the source is 64x48") {
		t.Fatalf("capture of a different size: %v", err)
	}
	silent := newPipelineFixture(t, true, "--snapshot-every=1h")
	silent.mustCall("start")
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, _, _, err := silent.control.call(ctx, sourceControlRequest{Operation: "capture"}); err == nil || !strings.Contains(err.Error(), "no pipeline snapshot became available") {
		t.Fatalf("capture without any snapshot: %v", err)
	}
}

func TestCaptureCanRequestAKeyframe(t *testing.T) {
	fixture := newPipelineFixture(t, true, "--snapshot-every=30ms")
	fixture.control.keyframeRestartAge = 200 * time.Millisecond
	fixture.mustCall("start")
	pid := readPID(t, fixture.pidFile)
	time.Sleep(300 * time.Millisecond)
	yes := true
	if _, _, _, err := fixture.capture(sourceControlRequest{RequestKeyframe: &yes}); err != nil {
		t.Fatal(err)
	}
	waitPIDChange(t, fixture.pidFile, pid)
}
