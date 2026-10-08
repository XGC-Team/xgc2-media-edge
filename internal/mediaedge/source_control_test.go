package mediaedge

import (
	"bytes"
	"context"
	"encoding/json"
	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
	"mime/multipart"
	"net"
	"net/http"
	"net/textproto"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDescribeSourceRejectsMissingFreshSnapshotCapability(t *testing.T) {
	description := defaultCaptureDescription()
	description.Capabilities = []string{"start", "stop", "request-keyframe", "capture"}
	control := newCaptureControlWithDescription(t, description)
	defer control.close()
	address := availableLoopbackRTPAddress(t)
	control.setRTPDestination(t, address)
	_, err := describeSource(context.Background(), SourceConfig{
		ID: "camera", RTPListenAddress: address, ControlSocket: control.socket,
	})
	if err == nil || !strings.Contains(err.Error(), `required capability "fresh-snapshot"`) {
		t.Fatalf("missing fresh snapshot capability error=%v", err)
	}
}

type captureControl struct {
	socket              string
	listener            net.Listener
	server              *http.Server
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
	target, _ := os.Hostname()
	control.description.ServiceRef = &xrpc.ServiceRef{TargetID: target, Service: "camera-source", APIVersion: "v1", InstanceID: "test-instance", Profile: xrpc.HTTP, Endpoint: xrpc.Endpoint{Kind: "unix", Address: socket}}
	control.server = &http.Server{Handler: http.HandlerFunc(control.handle), ReadHeaderTimeout: time.Second}
	go control.server.Serve(listener)
	return control
}

func (control *captureControl) handle(writer http.ResponseWriter, incoming *http.Request) {
	writer.Header().Set("X-Xrpc-Instance-ID", "test-instance")
	writer.Header().Set("Content-Type", "application/json")
	var request sourceControlRequest
	if incoming.Method != http.MethodGet && json.NewDecoder(incoming.Body).Decode(&request) != nil {
		writer.WriteHeader(400)
		return
	}
	request.Operation = pathBase(incoming.URL.Path)
	if incoming.URL.Path != "/v1/describe" && incoming.URL.Path != "/v1/media/sources/camera" && !strings.HasPrefix(incoming.URL.Path, "/v1/media/sources/camera/") {
		writer.WriteHeader(404)
		return
	}
	if incoming.URL.Path == "/v1/media/sources/camera/describe" {
		request.Operation = "describe"
	}
	if request.Operation == "describe" && incoming.Method != http.MethodGet {
		writer.WriteHeader(405)
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
		_, _ = writer.Write(encoded)
		return
	}
	if request.Operation != "capture" {
		state := "idle"
		if request.Operation == "start" {
			state = "active"
		}
		_ = json.NewEncoder(writer).Encode(sourceControlResponse{OK: true, ManagedSourceID: "camera", Active: request.Operation == "start", Completion: "applied", State: state, ConfigurationRevision: 1})
		return
	}
	rgb := make([]byte, 16*16*3)
	if request.IncludeRGB != nil && !*request.IncludeRGB {
		rgb = nil
	}
	jpeg := []byte("\xff\xd8xgc\xff\xd9")
	control.descriptionMu.RLock()
	renderPose := cloneSnapshotRenderPose(control.snapshotRenderPose)
	poseFrameID := control.snapshotPoseFrameID
	control.descriptionMu.RUnlock()
	response := sourceControlResponse{
		OK: true, SourceID: "camera", SnapshotID: request.SnapshotID, FrameID: "camera_optical", Sequence: 1,
		TimestampNanoseconds: 1700000000000000000, TimestampClockDomain: "simulation",
		Width: 16, Height: 16, PixelFormat: "rgb8", JPEGBytes: len(jpeg), RGBBytes: len(rgb),
		CameraMatrix: []float64{5, 0, 8, 0, 5, 8, 0, 0, 1}, Distortion: []float64{0, 0, 0, 0, 0},
		JPEGBackend: "source-jpeg-passthrough", JPEGReadback: "latest-compressed-frame",
		JPEGReadbackMillis: 0.25, JPEGEncodeMillis: 0,
		RenderPose: renderPose, PoseFrameID: poseFrameID,
	}
	contentType, body := cameraMultipartFixture(response, jpeg, rgb)
	writer.Header().Set("Content-Type", contentType)
	_, _ = writer.Write(body)
}

func defaultCaptureDescription() sourceControlResponse {
	return sourceControlResponse{
		OK: true, ProtocolVersion: sourceControlProtocolVersion, KeyframeRequestSupported: true, KeyframePolicy: "native-request",
		SourceID: "camera", Codec: sourceCodec,
		RTPPayloadType: sourceRTPPayloadType, RTPClockRate: h264RTPClockRate,
		RTPHost: "127.0.0.1", RTPPort: 5004,
		Width: 16, Height: 16, FPS: 20, FrameID: "camera_optical",
		Capabilities: append(append([]string(nil), requiredSourceCapabilities[:]...), "request-keyframe"),
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
		_ = control.server.Close()
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

func cameraMultipartFixture(metadata any, jpeg, rgb []byte) (string, []byte) {
	var buffer bytes.Buffer
	writer := multipart.NewWriter(&buffer)
	raw, _ := json.Marshal(metadata)
	for _, part := range []struct {
		name, kind string
		data       []byte
	}{{"metadata", "application/json", raw}, {"jpeg", "image/jpeg", jpeg}, {"rgb", "application/octet-stream", rgb}} {
		if part.name == "rgb" && len(part.data) == 0 {
			continue
		}
		header := textproto.MIMEHeader{}
		header.Set("Content-Type", part.kind)
		header.Set("Content-Disposition", `inline; name="`+part.name+`"`)
		out, _ := writer.CreatePart(header)
		out.Write(part.data)
	}
	writer.Close()
	return "multipart/mixed; boundary=" + writer.Boundary(), buffer.Bytes()
}
