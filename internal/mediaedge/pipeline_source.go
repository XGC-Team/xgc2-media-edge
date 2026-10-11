package mediaedge

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"io"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
)

const (
	// A pipeline that exits within this long after its start did not come up:
	// a missing device, a busy device, an element the platform lacks.
	pipelineStartSettle = 500 * time.Millisecond
	// SIGINT lets GStreamer release the device and the hardware encoder; what
	// has not exited after the grace is killed. The sum stays below the 2 s
	// that media-edge allows a lifecycle call.
	pipelineStopGrace = 1500 * time.Millisecond
	// The stream of a pipeline begins with an IDR and repeats one per GOP, which
	// every shipped template sets to one second. A restart to force another
	// IDR cannot beat that within this age of the pipeline.
	pipelineKeyframeRestartAge = 5 * time.Second
	pipelineSnapshotPoll       = 25 * time.Millisecond
	// How long the end of one process of a pipeline waits for the other's.
	pipelineExitWindow      = 100 * time.Millisecond
	pipelineDiagnosticBytes = 4096
	pipelineErrorBytes      = 400
)

var snapshotFileName = regexp.MustCompile(`^snapshot-([0-9]{5,})\.jpg$`)

type pipelineState string

const (
	pipelineIdle     pipelineState = "idle"
	pipelineStarting pipelineState = "starting"
	pipelineActive   pipelineState = "active"
	pipelineStopping pipelineState = "stopping"
	pipelineFaulted  pipelineState = "faulted"
)

// pipelineControl presents an encoder pipeline that media-edge runs itself as a
// camera-source: start spawns the pipeline, stop interrupts it, a keyframe
// request restarts it, and a capture reads the JPEG its snapshot branch wrote.
type pipelineControl struct {
	sourceID string
	config   SourceConfig
	spec     *pipelineSpec
	instance string
	output   io.Writer

	settle             time.Duration
	stopGrace          time.Duration
	keyframeRestartAge time.Duration

	// opMu serializes start, stop and restart. mu guards the state read by
	// status and capture, which must not wait for a spawn or a stop.
	opMu          sync.Mutex
	mu            sync.Mutex
	state         pipelineState
	desiredActive bool
	run           *pipelineRun
	generation    uint64
	startedAt     time.Time
	lastError     string
	closed        bool
}

// newPipelineControl expects a configuration normalized by Config.normalized,
// which resolved the template. output receives the diagnostics of the children.
func newPipelineControl(config SourceConfig, output io.Writer) *pipelineControl {
	if output == nil {
		output = os.Stderr
	}
	return &pipelineControl{
		sourceID: config.ID, config: config, spec: config.Pipeline.resolved,
		instance: rand.Text(), output: output,
		settle: pipelineStartSettle, stopGrace: pipelineStopGrace, keyframeRestartAge: pipelineKeyframeRestartAge,
		state: pipelineIdle,
	}
}

func (control *pipelineControl) boundInstance() string { return control.instance }

func (control *pipelineControl) boundReference() (xrpc.ServiceRef, bool) {
	return xrpc.ServiceRef{}, false
}

func (control *pipelineControl) call(ctx context.Context, request sourceControlRequest) (sourceControlResponse, []byte, []byte, error) {
	timeout := sourceControlRequestTimeout
	if request.Operation == "capture" {
		timeout = sourceSnapshotRequestTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	switch request.Operation {
	case "describe":
		return control.description(), nil, nil, nil
	case "status":
		return control.status(), nil, nil, nil
	case "config":
		return control.configuration(), nil, nil, nil
	case "start":
		if err := control.start(); err != nil {
			return sourceControlResponse{}, nil, nil, err
		}
		return control.lifecycleReceipt(""), nil, nil, nil
	case "stop":
		control.stop()
		return control.lifecycleReceipt(""), nil, nil, nil
	case "request-keyframe":
		completion, err := control.requestKeyframe()
		if err != nil {
			return sourceControlResponse{}, nil, nil, err
		}
		return control.lifecycleReceipt(completion), nil, nil, nil
	case "capture":
		return control.capture(ctx, request)
	default:
		return sourceControlResponse{}, nil, nil, fmt.Errorf("a pipeline source does not support %q", request.Operation)
	}
}

func (control *pipelineControl) description() sourceControlResponse {
	host, portText, _ := net.SplitHostPort(control.config.RTPListenAddress)
	port, _ := strconv.Atoi(portText)
	capabilities := []string{"start", "stop", "request-keyframe"}
	if control.spec.snapshotDir != "" {
		capabilities = append(capabilities, "capture", "fresh-snapshot")
	}
	return sourceControlResponse{
		OK: true, ProtocolVersion: sourceControlProtocolVersion, SourceID: control.sourceID,
		Codec: sourceCodec, RTPPayloadType: sourceRTPPayloadType, RTPClockRate: h264RTPClockRate,
		RTPHost: host, RTPPort: port,
		Width: control.config.Width, Height: control.config.Height, FPS: control.config.FPS, FrameID: control.config.FrameID,
		Capabilities: capabilities, KeyframeRequestSupported: true, KeyframePolicy: "restart",
	}
}

func (control *pipelineControl) status() sourceControlResponse {
	control.mu.Lock()
	defer control.mu.Unlock()
	return sourceControlResponse{
		OK: true, ManagedSourceID: control.sourceID, State: string(control.state),
		DesiredActive: control.desiredActive, AppliedActive: control.state == pipelineActive,
		LastError:             control.lastError,
		ConfigurationRevision: 1, DesiredRevision: 1, AppliedRevision: 1, Persistence: "ephemeral",
	}
}

func (control *pipelineControl) configuration() sourceControlResponse {
	host, portText, _ := net.SplitHostPort(control.config.RTPListenAddress)
	port, _ := strconv.Atoi(portText)
	current := sourceRuntimeConfiguration{RTPHost: host, RTPPort: port}
	return sourceControlResponse{
		OK: true, ManagedSourceID: control.sourceID, Desired: current, Applied: current,
		ConfigurationRevision: 1, DesiredRevision: 1, AppliedRevision: 1, Persistence: "ephemeral",
	}
}

func (control *pipelineControl) lifecycleReceipt(completion string) sourceControlResponse {
	control.mu.Lock()
	defer control.mu.Unlock()
	if completion == "" {
		completion = "applied"
	}
	return sourceControlResponse{
		OK: true, ManagedSourceID: control.sourceID, State: string(control.state),
		Active: control.state == pipelineActive, Completion: completion, ConfigurationRevision: 1,
	}
}

func (control *pipelineControl) start() error {
	control.opMu.Lock()
	defer control.opMu.Unlock()
	return control.startLocked()
}

func (control *pipelineControl) startLocked() error {
	control.mu.Lock()
	if control.closed {
		control.mu.Unlock()
		return errors.New("pipeline source is closed")
	}
	control.desiredActive = true
	if control.run != nil && control.state == pipelineActive {
		control.mu.Unlock()
		return nil
	}
	control.state = pipelineStarting
	control.mu.Unlock()
	if err := control.prepareScratch(); err != nil {
		return control.fail(err)
	}
	run, err := startPipelineRun(control.spec, control.output, control.sourceID)
	if err != nil {
		return control.fail(err)
	}
	control.mu.Lock()
	control.generation++
	control.run = run
	control.mu.Unlock()
	go func() {
		<-run.done
		control.runEnded(run)
	}()
	select {
	case <-run.done:
		control.runEnded(run)
		return fmt.Errorf("pipeline did not come up: %s", run.failure())
	case <-time.After(control.settle):
	}
	control.mu.Lock()
	defer control.mu.Unlock()
	if control.run == run && control.state == pipelineStarting {
		control.state = pipelineActive
		control.startedAt = time.Now()
		control.lastError = ""
	}
	return nil
}

func (control *pipelineControl) fail(err error) error {
	control.mu.Lock()
	defer control.mu.Unlock()
	control.state = pipelineFaulted
	control.lastError = err.Error()
	return err
}

// runEnded records the end of a run the owner did not end itself.
func (control *pipelineControl) runEnded(run *pipelineRun) {
	control.mu.Lock()
	defer control.mu.Unlock()
	if control.run != run {
		return
	}
	control.run = nil
	if run.stopping.Load() {
		control.state = pipelineIdle
		return
	}
	control.state = pipelineFaulted
	control.lastError = run.failure()
}

func (control *pipelineControl) stop() {
	control.opMu.Lock()
	defer control.opMu.Unlock()
	control.stopLocked()
}

func (control *pipelineControl) stopLocked() {
	control.mu.Lock()
	control.desiredActive = false
	run := control.run
	if run == nil {
		control.state = pipelineIdle
		control.lastError = ""
		control.mu.Unlock()
		control.clearScratch()
		return
	}
	control.state = pipelineStopping
	control.mu.Unlock()
	run.terminate(control.stopGrace)
	control.mu.Lock()
	if control.run == run {
		control.run = nil
	}
	control.state = pipelineIdle
	control.lastError = ""
	control.mu.Unlock()
	control.clearScratch()
}

// requestKeyframe restarts the pipeline, whose stream starts with an IDR. A
// request shortly after the start is coalesced: that IDR is the one asked for.
func (control *pipelineControl) requestKeyframe() (string, error) {
	control.opMu.Lock()
	defer control.opMu.Unlock()
	control.mu.Lock()
	if control.run == nil || control.state != pipelineActive {
		control.mu.Unlock()
		return "", errors.New("pipeline is not running")
	}
	if time.Since(control.startedAt) < control.keyframeRestartAge {
		control.mu.Unlock()
		return "coalesced", nil
	}
	run := control.run
	control.state = pipelineStopping
	control.mu.Unlock()
	run.terminate(control.stopGrace)
	control.mu.Lock()
	if control.run == run {
		control.run = nil
	}
	control.mu.Unlock()
	if err := control.startLocked(); err != nil {
		return "", err
	}
	return "restarted", nil
}

func (control *pipelineControl) close() {
	control.opMu.Lock()
	defer control.opMu.Unlock()
	control.mu.Lock()
	control.closed = true
	control.desiredActive = false
	run := control.run
	control.run = nil
	control.state = pipelineIdle
	control.mu.Unlock()
	if run != nil {
		run.terminate(control.stopGrace)
	}
	if control.spec.snapshotDir != "" {
		_ = os.RemoveAll(control.spec.snapshotDir)
	}
}

// prepareScratch makes the snapshot directory an empty directory owned by this
// process, so that no stale frame of an earlier run can be captured.
func (control *pipelineControl) prepareScratch() error {
	directory := control.spec.snapshotDir
	if directory == "" {
		return nil
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create pipeline scratch directory: %w", err)
	}
	info, err := os.Lstat(directory)
	if err != nil {
		return fmt.Errorf("inspect pipeline scratch directory: %w", err)
	}
	owner, known := info.Sys().(*syscall.Stat_t)
	if !info.IsDir() || !known || int(owner.Uid) != os.Geteuid() || info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("pipeline scratch %q must be a directory owned by this user with mode 0700", directory)
	}
	control.clearScratch()
	return nil
}

func (control *pipelineControl) clearScratch() {
	if control.spec.snapshotDir == "" {
		return
	}
	entries, err := os.ReadDir(control.spec.snapshotDir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if snapshotFileName.MatchString(entry.Name()) {
			_ = os.Remove(filepath.Join(control.spec.snapshotDir, entry.Name()))
		}
	}
}

type snapshotFrame struct {
	index   uint64
	data    []byte
	modTime time.Time
}

// newestSnapshot returns the newest complete JPEG of the snapshot branch. The
// file being written has no end marker yet and is skipped for the one before.
func (control *pipelineControl) newestSnapshot() (snapshotFrame, bool, error) {
	entries, err := os.ReadDir(control.spec.snapshotDir)
	if errors.Is(err, fs.ErrNotExist) {
		return snapshotFrame{}, false, nil
	}
	if err != nil {
		return snapshotFrame{}, false, err
	}
	var indexes []uint64
	for _, entry := range entries {
		if match := snapshotFileName.FindStringSubmatch(entry.Name()); match != nil {
			if index, err := strconv.ParseUint(match[1], 10, 64); err == nil {
				indexes = append(indexes, index)
			}
		}
	}
	sort.Slice(indexes, func(left, right int) bool { return indexes[left] > indexes[right] })
	for _, index := range indexes {
		path := filepath.Join(control.spec.snapshotDir, fmt.Sprintf(snapshotFilePattern, index))
		info, err := os.Stat(path)
		if err != nil || info.Size() < 4 || info.Size() > maximumCameraJPEGBytes {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil || !completeJPEG(data) {
			continue
		}
		return snapshotFrame{index: index, data: data, modTime: info.ModTime()}, true, nil
	}
	return snapshotFrame{}, false, nil
}

func completeJPEG(data []byte) bool {
	return len(data) >= 4 && data[0] == 0xff && data[1] == 0xd8 &&
		data[len(data)-2] == 0xff && data[len(data)-1] == 0xd9
}

func (control *pipelineControl) capture(ctx context.Context, request sourceControlRequest) (sourceControlResponse, []byte, []byte, error) {
	fail := func(err error) (sourceControlResponse, []byte, []byte, error) {
		return sourceControlResponse{}, nil, nil, err
	}
	if control.spec.snapshotDir == "" {
		return fail(ErrCaptureUnsupported)
	}
	if request.RequestKeyframe != nil && *request.RequestKeyframe {
		if _, err := control.requestKeyframe(); err != nil {
			return fail(err)
		}
	}
	admitted := time.Now()
	requireFresh := request.RequireFresh != nil && *request.RequireFresh
	for {
		frame, found, err := control.newestSnapshot()
		if err != nil {
			return fail(fmt.Errorf("read pipeline snapshot: %w", err))
		}
		// A frame completed after admission is newer than everything that was
		// visible at admission. The pipeline's own latency before the file is
		// complete is not part of that guarantee.
		if found && (!requireFresh || frame.modTime.After(admitted)) {
			return control.snapshotResponse(request, frame)
		}
		control.mu.Lock()
		running := control.run != nil
		control.mu.Unlock()
		if !running {
			return fail(errors.New("pipeline is not running"))
		}
		select {
		case <-ctx.Done():
			return fail(fmt.Errorf("no pipeline snapshot became available: %w", ctx.Err()))
		case <-time.After(pipelineSnapshotPoll):
		}
	}
}

func (control *pipelineControl) snapshotResponse(request sourceControlRequest, frame snapshotFrame) (sourceControlResponse, []byte, []byte, error) {
	fail := func(err error) (sourceControlResponse, []byte, []byte, error) {
		return sourceControlResponse{}, nil, nil, err
	}
	size, err := jpeg.DecodeConfig(bytes.NewReader(frame.data))
	if err != nil {
		return fail(fmt.Errorf("pipeline snapshot is not a JPEG: %w", err))
	}
	if size.Width != control.config.Width || size.Height != control.config.Height {
		return fail(fmt.Errorf("pipeline snapshot is %dx%d, the source is %dx%d", size.Width, size.Height, control.config.Width, control.config.Height))
	}
	var rgb []byte
	if request.IncludeRGB == nil || *request.IncludeRGB {
		if rgb, err = decodeRGB8(frame.data, size.Width, size.Height); err != nil {
			return fail(err)
		}
	}
	control.mu.Lock()
	generation := control.generation
	control.mu.Unlock()
	return sourceControlResponse{
		OK: true, SourceID: control.sourceID, SnapshotID: request.SnapshotID, FrameID: control.config.FrameID,
		// The frame index restarts with every run of the pipeline, so the
		// sequence carries the run in its upper half.
		Sequence:             generation<<32 | (frame.index + 1),
		TimestampNanoseconds: frame.modTime.UnixNano(), TimestampClockDomain: "system_realtime",
		Width: size.Width, Height: size.Height, PixelFormat: "rgb8",
		JPEGBytes: len(frame.data), RGBBytes: len(rgb),
		JPEGBackend: "pipeline", JPEGReadback: "snapshot-file",
	}, frame.data, rgb, nil
}

// decodeRGB8 returns the image as tightly packed RGB8, exactly width x height.
func decodeRGB8(data []byte, width, height int) ([]byte, error) {
	if int64(width)*int64(height)*3 > maximumCameraRGBBytes {
		return nil, errors.New("pipeline snapshot RGB exceeds the capture limit")
	}
	decoded, err := jpeg.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("decode pipeline snapshot: %w", err)
	}
	bounds := decoded.Bounds()
	if bounds.Dx() != width || bounds.Dy() != height {
		return nil, errors.New("pipeline snapshot changed size")
	}
	rgb := make([]byte, width*height*3)
	switch source := decoded.(type) {
	case *image.YCbCr:
		for row := 0; row < height; row++ {
			for column := 0; column < width; column++ {
				x, y := bounds.Min.X+column, bounds.Min.Y+row
				offset := (row*width + column) * 3
				rgb[offset], rgb[offset+1], rgb[offset+2] = color.YCbCrToRGB(
					source.Y[source.YOffset(x, y)], source.Cb[source.COffset(x, y)], source.Cr[source.COffset(x, y)])
			}
		}
	default:
		for row := 0; row < height; row++ {
			for column := 0; column < width; column++ {
				r, g, b, _ := decoded.At(bounds.Min.X+column, bounds.Min.Y+row).RGBA()
				offset := (row*width + column) * 3
				rgb[offset], rgb[offset+1], rgb[offset+2] = byte(r>>8), byte(g>>8), byte(b>>8)
			}
		}
	}
	return rgb, nil
}

// pipelineRun is one execution of a pipeline: the encoder command and, for a
// template that reads standard input, the producer feeding it.
type pipelineRun struct {
	// done is closed after every process exited and was reaped.
	done     chan struct{}
	stopping atomic.Bool

	processes []*pipelineProcess
	mu        sync.Mutex
	failed    string
}

type pipelineProcess struct {
	role        string
	command     *exec.Cmd
	diagnostics *diagnosticWriter

	mu     sync.Mutex
	reaped bool
}

// signal reaches the process group, so that a producer wrapper's own children
// do not outlive it holding the camera.
func (process *pipelineProcess) signal(signal syscall.Signal) {
	process.mu.Lock()
	defer process.mu.Unlock()
	if !process.reaped {
		_ = syscall.Kill(-process.command.Process.Pid, signal)
	}
}

// startPipelineRun spawns the processes of spec. It returns once they have been
// started; run.done reports their end.
func startPipelineRun(spec *pipelineSpec, output io.Writer, sourceID string) (*pipelineRun, error) {
	run := &pipelineRun{done: make(chan struct{})}
	started := make(chan error, 1)
	go run.supervise(spec, output, sourceID, started)
	if err := <-started; err != nil {
		return nil, err
	}
	return run, nil
}

func (run *pipelineRun) supervise(spec *pipelineSpec, output io.Writer, sourceID string, started chan<- error) {
	// The kernel sends the death signal of a child when the thread that
	// forked it exits, not when the process does. Keeping this goroutine on one
	// thread until the children are gone ties their lifetime to media-edge's:
	// a crash of media-edge cannot leave a pipeline holding the camera.
	runtime.LockOSThread()
	defer close(run.done)
	err := run.launch(spec, output, sourceID)
	started <- err
	if err != nil {
		return
	}
	type exit struct {
		process *pipelineProcess
		err     error
	}
	exits := make(chan exit, len(run.processes))
	for _, process := range run.processes {
		go func(process *pipelineProcess) {
			err := process.command.Wait()
			process.mu.Lock()
			process.reaped = true
			process.mu.Unlock()
			process.diagnostics.flush()
			exits <- exit{process, err}
		}(process)
	}
	// Either end ends the pipeline: the encoder cannot work without its input
	// and the producer has nothing to write to without the encoder. The other
	// end usually follows within moments because its pipe is gone; waiting for
	// it keeps the cause of the failure from being reported as its consequence.
	finished := []exit{<-exits}
	window := time.After(pipelineExitWindow)
collect:
	for len(finished) < len(run.processes) {
		select {
		case next := <-exits:
			finished = append(finished, next)
		case <-window:
			break collect
		}
	}
	if !run.stopping.Load() {
		reasons := make([]string, len(finished))
		for index, one := range finished {
			reasons[index] = describeExit(one.process, one.err)
		}
		run.mu.Lock()
		run.failed = strings.Join(reasons, "; ")
		run.mu.Unlock()
	}
	for _, process := range run.processes {
		process.signal(syscall.SIGINT)
	}
	for remaining := len(run.processes) - len(finished); remaining > 0; remaining-- {
		select {
		case <-exits:
		case <-time.After(pipelineStopGrace):
			for _, process := range run.processes {
				process.signal(syscall.SIGKILL)
			}
			<-exits
		}
	}
}

func describeExit(process *pipelineProcess, err error) string {
	text := process.role + " exited with status 0"
	if err != nil {
		text = process.role + " exited: " + err.Error()
	}
	if tail := process.diagnostics.tail(pipelineErrorBytes); tail != "" {
		text += ": " + tail
	}
	return text
}

func (run *pipelineRun) launch(spec *pipelineSpec, output io.Writer, sourceID string) error {
	newProcess := func(role string, arguments []string) *pipelineProcess {
		command := exec.Command(arguments[0], arguments[1:]...)
		// Own process group for signaling; the death signal for the case that
		// media-edge itself is gone.
		command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
		command.WaitDelay = time.Second
		diagnostics := &diagnosticWriter{prefix: fmt.Sprintf("pipeline %s %s: ", sourceID, role), output: output}
		command.Stderr = diagnostics
		return &pipelineProcess{role: role, command: command, diagnostics: diagnostics}
	}
	encoder := newProcess("encoder", spec.command)
	processes := []*pipelineProcess{encoder}
	if len(spec.producer) != 0 {
		reader, writer, err := os.Pipe()
		if err != nil {
			return fmt.Errorf("create the producer pipe: %w", err)
		}
		// The children hold their own copies once started.
		defer reader.Close()
		defer writer.Close()
		producer := newProcess("producer", spec.producer)
		encoder.command.Stdin = reader
		producer.command.Stdout = writer
		processes = append(processes, producer)
	}
	for index, process := range processes {
		if err := process.command.Start(); err != nil {
			for _, started := range processes[:index] {
				_ = started.command.Process.Kill()
				_ = started.command.Wait()
			}
			return fmt.Errorf("start the pipeline %s: %w", process.role, err)
		}
	}
	run.processes = processes
	return nil
}

// terminate interrupts the processes and kills what has not exited after the
// grace. It returns when all of them are gone.
func (run *pipelineRun) terminate(grace time.Duration) {
	run.stopping.Store(true)
	for _, process := range run.processes {
		process.signal(syscall.SIGINT)
	}
	select {
	case <-run.done:
		return
	case <-time.After(grace):
	}
	for _, process := range run.processes {
		process.signal(syscall.SIGKILL)
	}
	<-run.done
}

func (run *pipelineRun) failure() string {
	run.mu.Lock()
	defer run.mu.Unlock()
	if run.failed == "" {
		return "pipeline exited"
	}
	return "pipeline " + run.failed
}

// diagnosticWriter forwards the standard error of a child line by line to the
// media edge's log and keeps the tail for the status of a failed pipeline.
type diagnosticWriter struct {
	prefix string
	output io.Writer

	mu      sync.Mutex
	partial []byte
	recent  []byte
}

func (writer *diagnosticWriter) Write(data []byte) (int, error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	writer.recent = append(writer.recent, data...)
	if excess := len(writer.recent) - pipelineDiagnosticBytes; excess > 0 {
		writer.recent = append(writer.recent[:0], writer.recent[excess:]...)
	}
	writer.partial = append(writer.partial, data...)
	for {
		end := bytes.IndexByte(writer.partial, '\n')
		if end < 0 {
			break
		}
		writer.emit(writer.partial[:end])
		writer.partial = writer.partial[end+1:]
	}
	// A line that never ends is cut instead of accumulating.
	if len(writer.partial) > pipelineDiagnosticBytes {
		writer.emit(writer.partial)
		writer.partial = nil
	}
	return len(data), nil
}

func (writer *diagnosticWriter) emit(line []byte) {
	if text := strings.TrimRight(string(line), "\r"); text != "" {
		fmt.Fprintf(writer.output, "%s%s\n", writer.prefix, text)
	}
}

func (writer *diagnosticWriter) flush() {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	writer.emit(writer.partial)
	writer.partial = nil
}

// tail is the last bytes of the output as one line.
func (writer *diagnosticWriter) tail(limit int) string {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	recent := bytes.TrimSpace(writer.recent)
	if len(recent) > limit {
		recent = recent[len(recent)-limit:]
	}
	lines := strings.Fields(strings.ReplaceAll(string(recent), "\n", " | "))
	return strings.Join(lines, " ")
}
