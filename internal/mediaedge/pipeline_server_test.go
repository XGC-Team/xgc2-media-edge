package mediaedge

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/httpx"
)

// pipelineServer is a whole media edge, with MediaMTX faked, whose only source
// is the fake pipeline.
type pipelineServer struct {
	t       *testing.T
	server  *MediaMTXServer
	config  Config
	sink    *udpSink
	pidFile string
	scratch string
	logs    *lockedBuffer
}

func newPipelineServer(t *testing.T, snapshot bool, extra ...string) *pipelineServer {
	t.Helper()
	templates, runtime := t.TempDir(), t.TempDir()
	// Unix socket paths are short; t.TempDir() names can be long.
	socketDir, err := os.MkdirTemp("", "pe-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	sink := newUDPSink(t)
	pidFile := filepath.Join(runtime, "encoder.pid")
	command := []string{"fake-pipeline", "--rtp=${rtpHost}:${rtpPort}", "--pid-file=" + pidFile}
	if snapshot {
		command = append(command, "--snapshots=${snapshotDir}/"+snapshotFilePattern, "--snapshot-every=40ms")
	}
	writeTestTemplate(t, templates, testTemplate{name: "fake", input: "device", snapshot: snapshot, command: append(command, extra...)})
	config, err := (Config{
		TargetID: "pipeline-test", ControlAddress: "127.0.0.1:0", RPCSocket: filepath.Join(socketDir, "control.sock"),
		SessionGracePeriod: 200 * time.Millisecond, PipelineTemplatesDir: templates, PipelineRuntimeDir: filepath.Join(runtime, "pipelines"),
		Sources: []SourceConfig{pipelineTestSource("cam", sink.address(), "fake", nil, nil)},
	}).normalized()
	if err != nil {
		t.Fatal(err)
	}
	logs := &lockedBuffer{}
	server := newMediaMTXServer(config, MediaMTXSettings{Stderr: logs}, newFakeMediaMTXControl("cam"), newFakeMediaMTXProcess())
	if err := server.Start(); err != nil {
		t.Fatalf("start the media edge: %v", err)
	}
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("close the media edge: %v", err)
		}
	})
	return &pipelineServer{t: t, server: server, config: config, sink: sink, pidFile: pidFile, scratch: config.Sources[0].Pipeline.resolved.snapshotDir, logs: logs}
}

func (edge *pipelineServer) status() SourceStatus {
	edge.t.Helper()
	return edge.server.SourceStatuses()[0]
}

func (edge *pipelineServer) waitStatus(description string, condition func(SourceStatus) bool) SourceStatus {
	edge.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if status := edge.status(); condition(status) {
			return status
		}
		time.Sleep(10 * time.Millisecond)
	}
	edge.t.Fatalf("source never became %s: %+v logs %q", description, edge.status(), edge.logs.String())
	return SourceStatus{}
}

func TestMediaEdgeRunsAPipelineOnlyWhileItIsWatched(t *testing.T) {
	edge := newPipelineServer(t, true)
	if _, err := os.Stat(edge.pidFile); err == nil {
		t.Fatal("the pipeline runs before anyone watches")
	}
	status := edge.waitStatus("controlled", func(s SourceStatus) bool { return s.ControlHealthy && s.NativeState == "idle" })
	if status.Kind != SourceKindPipeline || status.Active || status.Width != 64 || status.Height != 48 || status.FPS != 10 || status.FrameID != "cam_optical" {
		t.Fatalf("idle status = %+v", status)
	}

	answer, err := edge.server.OpenSession(context.Background(), "cam", SessionOffer{SDP: "v=0\r\n"})
	if err != nil {
		t.Fatal(err)
	}
	first := readPID(t, edge.pidFile)
	if !edge.sink.waitFor(5, 3*time.Second) {
		t.Fatalf("no RTP reached the MediaMTX port; logs %q", edge.logs.String())
	}
	status = edge.waitStatus("active", func(s SourceStatus) bool { return s.NativeState == "active" && s.NativeAppliedActive })
	if !status.Active || status.Viewers != 1 || status.StateUncertain {
		t.Fatalf("watched status = %+v", status)
	}

	// A calibration capture reads the snapshot branch while the stream runs.
	yes := true
	snapshot, err := edge.server.CaptureSnapshot(context.Background(), "cam", SnapshotCaptureRequest{RequireFresh: &yes})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Width != 64 || snapshot.Height != 48 || snapshot.FrameID != "cam_optical" || snapshot.PixelFormat != "rgb8" ||
		len(snapshot.JPEG) == 0 || len(snapshot.RGB) != 64*48*3 || snapshot.Sequence == 0 ||
		snapshot.TimestampClockDomain != "system_realtime" || snapshot.CalibrationState != "unavailable" {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	if readPID(t, edge.pidFile) != first {
		t.Fatal("the capture restarted the pipeline")
	}

	// The last viewer leaves: after the grace period the pipeline stops.
	if found, err := edge.server.CloseSession(answer.SessionID); err != nil || !found {
		t.Fatalf("close session: %v %v", found, err)
	}
	edge.waitStatus("idle", func(s SourceStatus) bool { return !s.Active && s.NativeState == "idle" })
	if !waitProcessGone(t, first, 3*time.Second) {
		t.Fatal("the pipeline keeps running without viewers")
	}
	if entries, _ := os.ReadDir(edge.scratch); len(entries) != 0 {
		t.Fatalf("%d snapshots left behind", len(entries))
	}

	// A new viewer starts it again.
	if _, err := edge.server.OpenSession(context.Background(), "cam", SessionOffer{SDP: "v=0\r\n"}); err != nil {
		t.Fatal(err)
	}
	waitPIDChange(t, edge.pidFile, first)
	if !edge.sink.waitFor(edge.sink.received.Load()+3, 3*time.Second) {
		t.Fatal("the restarted pipeline sends no RTP")
	}
}

func TestMediaEdgeStopsTheWholePipelineWhenItCloses(t *testing.T) {
	templates, runtime := t.TempDir(), t.TempDir()
	sink := newUDPSink(t)
	encoderPID, producerPID := filepath.Join(runtime, "encoder.pid"), filepath.Join(runtime, "producer.pid")
	writeTestTemplate(t, templates, testTemplate{
		name: "fake", input: "stdin", snapshot: false,
		command: []string{"fake-pipeline", "--stdin", "--rtp=${rtpHost}:${rtpPort}", "--pid-file=" + encoderPID},
	})
	producer := []string{testBinary(t), "fake-producer", "--pid-file=" + producerPID, "--every=10ms"}
	config, err := (Config{
		ControlAddress: "127.0.0.1:0", SessionGracePeriod: time.Hour, PipelineTemplatesDir: templates, PipelineRuntimeDir: filepath.Join(runtime, "pipelines"),
		Sources: []SourceConfig{pipelineTestSource("cam", sink.address(), "fake", nil, producer)},
	}).normalized()
	if err != nil {
		t.Fatal(err)
	}
	server := newMediaMTXServer(config, MediaMTXSettings{Stderr: &lockedBuffer{}}, newFakeMediaMTXControl("cam"), newFakeMediaMTXProcess())
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := server.OpenSession(context.Background(), "cam", SessionOffer{SDP: "v=0\r\n"}); err != nil {
		t.Fatal(err)
	}
	encoder, feeder := readPID(t, encoderPID), readPID(t, producerPID)
	if !sink.waitFor(3, 3*time.Second) {
		t.Fatal("no RTP reached the MediaMTX port")
	}
	if err := server.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if !processGone(encoder) || !processGone(feeder) {
		t.Fatal("closing the media edge left part of the pipeline running")
	}
	if _, err := os.Stat(config.Sources[0].Pipeline.resolved.snapshotDir); err == nil {
		t.Fatal("closing the media edge left a scratch directory")
	}
}

func TestMediaEdgeRunsAndReapsATemplateOwnedProducerPair(t *testing.T) {
	templates, runtime := t.TempDir(), t.TempDir()
	sink := newUDPSink(t)
	encoderPID, producerPID := filepath.Join(runtime, "encoder.pid"), filepath.Join(runtime, "producer.pid")
	writeTestTemplate(t, templates, testTemplate{
		name: "owned", input: "stdin", snapshot: true,
		command: []string{"fake-pipeline", "--stdin", "--rtp=${rtpHost}:${rtpPort}", "--size=${width}x${height}",
			"--snapshots=${snapshotDir}/" + snapshotFilePattern, "--pid-file=" + encoderPID},
		producer: []string{"fake-producer", "--pid-file=" + producerPID, "--every=10ms"},
	})
	config, err := (Config{
		ControlAddress: "127.0.0.1:0", SessionGracePeriod: time.Hour, PipelineTemplatesDir: templates, PipelineRuntimeDir: filepath.Join(runtime, "pipelines"),
		Sources: []SourceConfig{pipelineTestSource("cam", sink.address(), "owned", nil, nil)},
	}).normalized()
	if err != nil {
		t.Fatal(err)
	}
	server := newMediaMTXServer(config, MediaMTXSettings{Stderr: &lockedBuffer{}}, newFakeMediaMTXControl("cam"), newFakeMediaMTXProcess())
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{encoderPID, producerPID} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("a process started without a consumer: %s %v", path, err)
		}
	}
	description, _, _, err := server.sources["cam"].controller().call(context.Background(), sourceControlRequest{Operation: "describe"})
	_, port, _ := net.SplitHostPort(sink.address())
	if err != nil || description.Codec != "H264" || description.RTPPayloadType != 96 || description.RTPClockRate != 90000 ||
		description.RTPHost != "127.0.0.1" || strconv.Itoa(description.RTPPort) != port ||
		description.Width != 64 || description.Height != 48 || description.FPS != 10 || description.FrameID != "cam_optical" ||
		description.KeyframePolicy != "restart" || !description.KeyframeRequestSupported ||
		!slices.Equal(description.Capabilities, []string{"start", "stop", "request-keyframe", "capture", "fresh-snapshot"}) {
		t.Fatalf("describe = %+v %v", description, err)
	}
	if _, err := server.OpenSession(context.Background(), "cam", SessionOffer{SDP: "v=0\r\n"}); err != nil {
		t.Fatal(err)
	}
	encoder, feeder := readPID(t, encoderPID), readPID(t, producerPID)
	if encoder == feeder || !sink.waitFor(3, 3*time.Second) {
		t.Fatal("the producer and encoder pair did not start and send RTP")
	}
	yes := true
	snapshot, err := server.CaptureSnapshot(context.Background(), "cam", SnapshotCaptureRequest{RequireFresh: &yes})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Width != 64 || snapshot.Height != 48 || snapshot.FrameID != "cam_optical" || snapshot.PixelFormat != "rgb8" ||
		len(snapshot.JPEG) == 0 || len(snapshot.RGB) != 64*48*3 || snapshot.Sequence == 0 ||
		snapshot.TimestampClockDomain != "system_realtime" || snapshot.CalibrationState != "unavailable" {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	for _, pid := range []int{encoder, feeder} {
		if _, err := os.Stat(filepath.Join("/proc", strconv.Itoa(pid))); !os.IsNotExist(err) {
			t.Fatalf("process %d was not reaped: %v", pid, err)
		}
	}
	if _, err := os.Stat(config.Sources[0].Pipeline.resolved.snapshotDir); !os.IsNotExist(err) {
		t.Fatalf("scratch directory remains: %v", err)
	}
}

// rpcClient discovers the private media-edge service and binds its incarnation.
func (edge *pipelineServer) rpcClient() *httpx.Client {
	edge.t.Helper()
	target := edge.config.TargetID
	reference := xrpc.ServiceRef{TargetID: target, Service: "media-edge", APIVersion: "v1", Profile: xrpc.HTTP, Endpoint: xrpc.Endpoint{Kind: "unix", Address: edge.config.RPCSocket}}
	discovery, err := httpx.New(httpx.Config{LocalTargetID: target, Service: reference, MaxResponseBytes: 1 << 20})
	if err != nil {
		edge.t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	data, status, _, err := discovery.Do(ctx, http.MethodGet, "/v1/describe", "test:discover", "", nil)
	discovery.Close()
	if err != nil || status != http.StatusOK {
		edge.t.Fatalf("discover the media edge: %d %v %s", status, err, data)
	}
	var description struct {
		Service xrpc.ServiceRef `json:"service_ref"`
	}
	if err := json.Unmarshal(data, &description); err != nil {
		edge.t.Fatal(err)
	}
	client, err := httpx.New(httpx.Config{LocalTargetID: target, Service: description.Service, MaxResponseBytes: 8 << 20})
	if err != nil {
		edge.t.Fatal(err)
	}
	edge.t.Cleanup(client.Close)
	return client
}

func TestPrivateRPCCapturesFromAPipelineSource(t *testing.T) {
	edge := newPipelineServer(t, true)
	client := edge.rpcClient()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	reply, err := client.DoStream(ctx, http.MethodPost, "/v1/media/sources/cam/capture", "test:capture", "application/json",
		[]byte(`{"includeRgb":true,"requestKeyframe":false,"requireFresh":true}`))
	if err != nil {
		t.Fatal(err)
	}
	defer reply.Body.Close()
	if reply.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(reply.Body)
		t.Fatalf("capture HTTP %d: %s", reply.StatusCode, body)
	}
	metadata, jpegBytes, rgb, err := readCameraMultipart(reply.Header.Get("Content-Type"), reply.Body, true)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.SourceID != "cam" || metadata.Width != 64 || metadata.Height != 48 || metadata.Sequence == 0 ||
		len(rgb) != 64*48*3 || !bytes.HasPrefix(jpegBytes, []byte{0xff, 0xd8}) || metadata.CalibrationState != "unavailable" {
		t.Fatalf("capture metadata = %+v", metadata)
	}
	if _, status, _, err := client.Do(ctx, http.MethodDelete, "/v1/media/snapshots/"+metadata.SnapshotID, "test:release", "", nil); err != nil || status != http.StatusNoContent {
		t.Fatalf("release snapshot: %d %v", status, err)
	}

	// A pipeline is not a service: there is no reference to hand out.
	data, status, _, err := client.Do(ctx, http.MethodGet, "/v1/media/sources/cam/ref", "test:ref", "", nil)
	if err != nil || status != http.StatusNotFound {
		t.Fatalf("source reference of a pipeline: %d %v %s", status, err, data)
	}
}

func TestPrivateRPCRefusesCaptureFromAPipelineWithoutSnapshotBranch(t *testing.T) {
	edge := newPipelineServer(t, false)
	client := edge.rpcClient()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	data, status, _, err := client.Do(ctx, http.MethodPost, "/v1/media/sources/cam/capture", "test:capture", "application/json", []byte(`{}`))
	if err != nil || status != http.StatusNotImplemented {
		t.Fatalf("capture without a snapshot branch: %d %v %s", status, err, data)
	}
	if _, err := os.Stat(edge.pidFile); err == nil {
		t.Fatal("a refused capture still started the pipeline")
	}
}
