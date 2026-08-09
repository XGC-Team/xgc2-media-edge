package mediaedge

import (
	"bufio"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

type captureControl struct {
	socket              string
	listener            net.Listener
	requests            chan sourceControlRequest
	closed              chan struct{}
	beforeReply         func(sourceControlRequest)
	descriptionMu       sync.RWMutex
	description         sourceControlResponse
	snapshotRenderPose  *SnapshotRenderPose
	snapshotPoseFrameID string
	once                sync.Once
}

func newCaptureControl(t *testing.T) *captureControl {
	t.Helper()
	return newCaptureControlAtWithHook(t, filepath.Join(t.TempDir(), "camera.sock"), nil)
}

func newCaptureControlWithHook(t *testing.T, beforeReply func(sourceControlRequest)) *captureControl {
	t.Helper()
	return newCaptureControlAtWithHook(t, filepath.Join(t.TempDir(), "camera.sock"), beforeReply)
}

func newCaptureControlWithDescription(t *testing.T, description sourceControlResponse) *captureControl {
	t.Helper()
	return newCaptureControlAtWithHookAndDescription(t, filepath.Join(t.TempDir(), "camera.sock"), nil, description)
}

func newCaptureControlAt(t *testing.T, socket string) *captureControl {
	t.Helper()
	return newCaptureControlAtWithHook(t, socket, nil)
}

func newCaptureControlAtWithHook(t *testing.T, socket string, beforeReply func(sourceControlRequest)) *captureControl {
	t.Helper()
	return newCaptureControlAtWithHookAndDescription(t, socket, beforeReply, defaultCaptureDescription())
}

func newCaptureControlAtWithHookAndDescription(
	t *testing.T,
	socket string,
	beforeReply func(sourceControlRequest),
	description sourceControlResponse,
) *captureControl {
	t.Helper()
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("listen capture control: %v", err)
	}
	control := &captureControl{
		socket: socket, listener: listener, requests: make(chan sourceControlRequest, 16),
		closed: make(chan struct{}), beforeReply: beforeReply, description: description,
	}
	go control.accept()
	return control
}

func (control *captureControl) accept() {
	for {
		connection, err := control.listener.Accept()
		if err != nil {
			return
		}
		go control.handle(connection)
	}
}

func (control *captureControl) handle(connection net.Conn) {
	defer connection.Close()
	line, err := bufio.NewReader(connection).ReadString('\n')
	if err != nil {
		return
	}
	var request sourceControlRequest
	if json.Unmarshal([]byte(line), &request) != nil {
		return
	}
	select {
	case control.requests <- request:
	case <-control.closed:
		return
	}
	if control.beforeReply != nil {
		control.beforeReply(request)
	}
	if request.Operation == "describe" {
		control.descriptionMu.RLock()
		description := control.description
		control.descriptionMu.RUnlock()
		encoded, _ := json.Marshal(description)
		_, _ = connection.Write(append(encoded, '\n'))
		return
	}
	if request.Operation != "snapshot" {
		_, _ = connection.Write([]byte("{\"ok\":true}\n"))
		return
	}
	rgb := make([]byte, 16*16*3)
	jpeg := []byte("\xff\xd8xgc\xff\xd9")
	control.descriptionMu.RLock()
	renderPose := cloneSnapshotRenderPose(control.snapshotRenderPose)
	poseFrameID := control.snapshotPoseFrameID
	control.descriptionMu.RUnlock()
	response := sourceControlResponse{
		OK: true, SnapshotID: request.SnapshotID, FrameID: "camera_optical",
		TimestampNanoseconds: 1700000000000000000, TimestampClockDomain: "simulation",
		Width: 16, Height: 16, PixelFormat: "rgb8", JPEGBytes: len(jpeg), RGBBytes: len(rgb),
		CameraMatrix: []float64{5, 0, 8, 0, 5, 8, 0, 0, 1}, Distortion: []float64{0, 0, 0, 0, 0},
		RenderPose: renderPose, PoseFrameID: poseFrameID,
	}
	encoded, _ := json.Marshal(response)
	_, _ = connection.Write(append(encoded, '\n'))
	_, _ = connection.Write(jpeg)
	_, _ = connection.Write(rgb)
}

func defaultCaptureDescription() sourceControlResponse {
	return sourceControlResponse{
		OK: true, ProtocolVersion: sourceControlProtocolVersion,
		SourceID: "camera", Codec: sourceCodec,
		RTPPayloadType: sourceRTPPayloadType, RTPClockRate: h264RTPClockRate,
		RTPHost: "127.0.0.1", RTPPort: 5004,
		Width: 16, Height: 16, FPS: 20, FrameID: "camera_optical",
		Capabilities: append([]string(nil), requiredSourceCapabilities[:]...),
	}
}

func availableLoopbackRTPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatalf("allocate test RTP port: %v", err)
	}
	address := listener.LocalAddr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release test RTP port: %v", err)
	}
	return address
}

func (control *captureControl) setRTPDestination(t *testing.T, address string) {
	t.Helper()
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatalf("split test RTP address: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse test RTP port: %v", err)
	}
	control.descriptionMu.Lock()
	control.description.RTPHost = host
	control.description.RTPPort = port
	control.descriptionMu.Unlock()
}

func (control *captureControl) waitFor(t *testing.T, occurrences int, predicate func(sourceControlRequest) bool) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	found := 0
	for found < occurrences {
		select {
		case request := <-control.requests:
			if predicate(request) {
				found++
			}
		case <-deadline:
			t.Fatalf("capture control did not receive %d matching request(s)", occurrences)
		}
	}
}

func (control *captureControl) expectNoMatch(t *testing.T, duration time.Duration, predicate func(sourceControlRequest) bool) {
	t.Helper()
	timer := time.NewTimer(duration)
	defer timer.Stop()
	for {
		select {
		case request := <-control.requests:
			if predicate(request) {
				t.Fatalf("capture control unexpectedly received matching request: %+v", request)
			}
		case <-timer.C:
			return
		}
	}
}

func (control *captureControl) close() {
	control.once.Do(func() {
		close(control.closed)
		_ = control.listener.Close()
		_ = os.Remove(control.socket)
	})
}

func eventually(t *testing.T, timeout time.Duration, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition did not become true before timeout")
}
