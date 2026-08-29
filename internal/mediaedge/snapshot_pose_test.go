package mediaedge

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestCaptureSnapshotPassesThroughOptionalSourceRenderPose(t *testing.T) {
	capture := newCaptureControl(t)
	defer capture.close()
	capture.descriptionMu.Lock()
	capture.snapshotRenderPose = &SnapshotRenderPose{
		Position:    SnapshotVector3{X: 4, Y: 5, Z: 6},
		Orientation: SnapshotQuaternion{W: 1},
	}
	capture.snapshotPoseFrameID = "gazebo_world"
	capture.descriptionMu.Unlock()
	rtpAddress := availableLoopbackRTPAddress(t)
	capture.setRTPDestination(t, rtpAddress)
	config, err := (Config{
		ControlAddress: "127.0.0.1:0", SessionGracePeriod: 5 * time.Millisecond,
		Sources: []SourceConfig{{ID: "camera", RTPListenAddress: rtpAddress, ControlSocket: capture.socket}},
	}).normalized()
	if err != nil {
		t.Fatalf("normalize MediaMTX snapshot server: %v", err)
	}
	server := newMediaMTXServer(config, MediaMTXSettings{}, newFakeMediaMTXControl("camera"), newFakeMediaMTXProcess())
	if err := server.Start(); err != nil {
		t.Fatalf("start MediaMTX snapshot server: %v", err)
	}
	defer server.Close()

	includeRGB := false
	requestKeyframe := false
	requireFresh := true
	snapshot, err := server.CaptureSnapshot(context.Background(), "camera", SnapshotCaptureRequest{
		IncludeRGB: &includeRGB, RequestKeyframe: &requestKeyframe, RequireFresh: &requireFresh,
	})
	if err != nil {
		t.Fatalf("capture snapshot with render pose: %v", err)
	}
	if snapshot.RenderPose == nil || snapshot.PoseFrameID != "gazebo_world" ||
		snapshot.RenderPose.Position.Y != 5 || snapshot.RenderPose.Orientation.W != 1 ||
		snapshot.TimestampClockDomain != "simulation" {
		t.Fatalf("captured render pose = %+v frame=%q", snapshot.RenderPose, snapshot.PoseFrameID)
	}
	if len(snapshot.RGB) != 0 || len(snapshot.JPEG) == 0 {
		t.Fatalf("JPEG-only snapshot payload RGB=%d JPEG=%d", len(snapshot.RGB), len(snapshot.JPEG))
	}
	if snapshot.JPEGBackend != "source-jpeg-passthrough" ||
		snapshot.JPEGReadback != "latest-compressed-frame" ||
		snapshot.JPEGReadbackMillis != 0.25 || snapshot.JPEGEncodeMillis != 0 {
		t.Fatalf("snapshot JPEG diagnostics were not preserved: %+v", snapshot)
	}
	capture.waitFor(t, 1, func(request sourceControlRequest) bool {
		return request.Operation == "snapshot" && request.IncludeRGB != nil && !*request.IncludeRGB &&
			request.RequestKeyframe != nil && !*request.RequestKeyframe &&
			request.RequireFresh != nil && *request.RequireFresh
	})
	metadata := snapshot.metadata()
	if metadata.RenderPose == nil || metadata.PoseFrameID != "gazebo_world" ||
		metadata.TimestampClockDomain != "simulation" ||
		metadata.JPEGBackend != "source-jpeg-passthrough" ||
		metadata.JPEGReadback != "latest-compressed-frame" {
		t.Fatalf("snapshot metadata lost render pose: %+v", metadata)
	}
}

func TestSnapshotMetadataOptionallyCarriesRenderPose(t *testing.T) {
	withoutPose, err := json.Marshal((Snapshot{ID: "plain"}).metadata())
	if err != nil {
		t.Fatalf("encode snapshot without pose: %v", err)
	}
	if strings.Contains(string(withoutPose), "renderPose") ||
		strings.Contains(string(withoutPose), "poseFrameId") {
		t.Fatalf("optional pose fields were not omitted: %s", withoutPose)
	}

	snapshot := Snapshot{
		ID: "posed",
		RenderPose: &SnapshotRenderPose{
			Position:    SnapshotVector3{X: 1, Y: 2, Z: 3},
			Orientation: SnapshotQuaternion{X: 0.1, Y: 0.2, Z: 0.3, W: 0.9},
		},
		PoseFrameID: "world",
	}
	metadata := snapshot.metadata()
	if metadata.RenderPose == nil || metadata.PoseFrameID != "world" ||
		metadata.RenderPose.Position.Z != 3 || metadata.RenderPose.Orientation.W != 0.9 {
		t.Fatalf("snapshot render pose was not preserved: %+v", metadata)
	}
	// metadata returns an independent pointer so callers cannot mutate the
	// immutable snapshot retained by Edge.
	metadata.RenderPose.Position.X = 99
	if snapshot.RenderPose.Position.X != 1 {
		t.Fatal("snapshot render pose was aliased")
	}
}
