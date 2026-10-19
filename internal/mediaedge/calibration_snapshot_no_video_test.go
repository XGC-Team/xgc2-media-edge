package mediaedge

import (
	"context"
	"testing"
	"time"
)

func newNativeSnapshotServer(t *testing.T) (*captureControl, *MediaMTXServer) {
	t.Helper()
	camera := newCaptureControl(t)
	t.Cleanup(camera.close)
	address := availableLoopbackRTPAddress(t)
	camera.setRTPDestination(t, address)
	config, err := (Config{ControlAddress: "127.0.0.1:0", Sources: []SourceConfig{{
		ID: "camera", RTPListenAddress: address, ControlSocket: camera.socket,
	}}}).normalized()
	if err != nil {
		t.Fatal(err)
	}
	server := newMediaMTXServer(config, MediaMTXSettings{}, newFakeMediaMTXControl("camera"), newFakeMediaMTXProcess())
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	return camera, server
}

// A calibration JPEG must work without an H264 encoder lease. The native
// simulator can render a fresh frame and encode JPEG on the CPU independently.
func TestCalibrationSnapshotDoesNotActivateLiveEncoder(t *testing.T) {
	camera, server := newNativeSnapshotServer(t)
	no, yes := false, true
	snapshot, err := server.CaptureSnapshot(context.Background(), "camera", SnapshotCaptureRequest{
		IncludeRGB: &no, RequestKeyframe: &no, RequireFresh: &yes,
	})
	if err != nil || len(snapshot.JPEG) == 0 || len(snapshot.RGB) != 0 {
		t.Fatalf("JPEG-only snapshot: bytes=%d RGB=%d error=%v", len(snapshot.JPEG), len(snapshot.RGB), err)
	}
	camera.expectNoMatch(t, 40*time.Millisecond, activeControlRequest(true))
}

// A keyframe request needs the live encoder, so the lease path must remain.
func TestSnapshotKeyframeRequestActivatesLiveEncoder(t *testing.T) {
	camera, server := newNativeSnapshotServer(t)
	no, yes := false, true
	if _, err := server.CaptureSnapshot(context.Background(), "camera", SnapshotCaptureRequest{
		IncludeRGB: &no, RequestKeyframe: &yes, RequireFresh: &yes,
	}); err != nil {
		t.Fatalf("keyframe snapshot: %v", err)
	}
	camera.waitFor(t, 1, activeControlRequest(true))
}
