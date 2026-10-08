package mediaedge

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func TestCameraMultipartRejectsOversizeNativeHeadersAndForbiddenRGB(t *testing.T) {
	metadata := sourceControlResponse{OK: true, JPEGBytes: 3, RGBBytes: 2}
	kind, wire := cameraMultipartFixture(metadata, []byte{1, 2, 3}, []byte{4, 5})
	for name, input := range map[string][]byte{
		"preamble":  append([]byte(strings.Repeat("p", maximumCameraPartHeaderBytes+4096)+"\r\n"), wire...),
		"header":    bytes.Replace(wire, []byte("Content-Type: application/json"), []byte("X-Padding: "+strings.Repeat("p", maximumCameraPartHeaderBytes+4096)+"\r\nContent-Type: application/json"), 1),
		"transfer":  bytes.Replace(wire, []byte("Content-Type: application/json"), []byte("Content-Transfer-Encoding: base64\r\nContent-Type: application/json"), 1),
		"duplicate": bytes.Replace(wire, []byte("Content-Type: application/json"), []byte("Content-Type: application/json\r\nContent-Type: application/json"), 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, _, err := readCameraMultipart(kind, bytes.NewReader(input), true); err == nil {
				t.Fatal("invalid MIME accepted")
			}
		})
	}
	if _, _, _, err := readCameraMultipart(kind, bytes.NewReader(wire), false); err == nil {
		t.Fatal("JPEG-only caller accepted unwanted RGB")
	}
	if _, _, _, err := readCameraMultipartWithLimit(kind, bytes.NewReader(wire), true, 4); err == nil || !strings.Contains(err.Error(), "byte budget") {
		t.Fatalf("capture budget ignored: %v", err)
	}
}

func TestMediaOperationAndSessionAdmissionRejectsBeforeSourceWork(t *testing.T) {
	server := newMediaMTXServer(Config{MaxOperations: 1, MaxSessions: 1, Sources: []SourceConfig{{ID: "camera"}}}, MediaMTXSettings{}, newFakeMediaMTXControl("camera"), newFakeMediaMTXProcess())
	_, finish, err := server.beginOperation(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = server.beginOperation(context.Background()); !errors.Is(err, ErrMediaCapacity) {
		t.Fatalf("operation admission=%v", err)
	}
	finish()
	server.sessions["existing"] = &mediaMTXSession{}
	if _, err = server.OpenSession(context.Background(), "camera", SessionOffer{SDP: "offer"}); !errors.Is(err, ErrMediaCapacity) {
		t.Fatalf("session admission=%v", err)
	}
	if server.sources["camera"].controlRPC != nil || server.sources["camera"].pending != 0 || server.activeOperations != 0 {
		t.Fatal("rejected call reached source work or retained admission")
	}
}

func TestMediaCaptureAdmissionRetainsOneActualJob(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	capture := newCaptureControlWithHook(t, func(request sourceControlRequest) {
		if request.Operation == "capture" {
			close(entered)
			<-release
		}
	})
	defer capture.close()
	address := availableLoopbackRTPAddress(t)
	capture.setRTPDestination(t, address)
	config, err := (Config{ControlAddress: "127.0.0.1:0", SessionGracePeriod: time.Hour, Sources: []SourceConfig{{ID: "camera", RTPListenAddress: address, ControlSocket: capture.socket}}}).normalized()
	if err != nil {
		t.Fatal(err)
	}
	server := newMediaMTXServer(config, MediaMTXSettings{}, newFakeMediaMTXControl("camera"), newFakeMediaMTXProcess())
	if err = server.Start(); err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	completed := make(chan error, 1)
	go func() {
		_, err := server.CaptureSnapshot(context.Background(), "camera", SnapshotCaptureRequest{})
		completed <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("capture did not start")
	}
	if _, err = server.CaptureSnapshot(context.Background(), "camera", SnapshotCaptureRequest{}); !errors.Is(err, ErrMediaCapacity) {
		t.Fatalf("second capture=%v", err)
	}
	close(release)
	if err = <-completed; err != nil {
		t.Fatal(err)
	}
}

func TestMediaCloseRetriesLostNativeStopReceipt(t *testing.T) {
	probe := newLifecycleControlProbe(t)
	probe.failStops.Store(1)
	server := newMediaMTXServer(Config{Sources: []SourceConfig{{ID: "camera", ControlSocket: probe.socket, ControlInstanceID: "test-instance"}}}, MediaMTXSettings{}, newFakeMediaMTXControl("camera"), newFakeMediaMTXProcess())
	source := server.sources["camera"]
	source.active = true
	if err := server.Close(); err == nil {
		t.Fatal("lost native stop reply reported closed")
	}
	source.mu.Lock()
	active, uncertain := source.active, source.deactivateUncertain
	source.mu.Unlock()
	if !active || !uncertain || server.closeComplete || probe.stops.Load() != 1 {
		t.Fatal("failed close released native source ownership")
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if probe.stops.Load() != 2 || !server.closeComplete || source.active || source.deactivateUncertain {
		t.Fatal("second close failed to confirm and release source")
	}
	if err := server.Close(); err != nil || probe.stops.Load() != 2 {
		t.Fatal("confirmed close sent another mutation")
	}
}

func TestSnapshotRetentionHasProductWideByteBudget(t *testing.T) {
	server := newMediaMTXServer(Config{MaxCaptureBytes: 4, MaxRetainedSnapshotBytes: 8, Sources: []SourceConfig{{ID: "a"}, {ID: "b"}}}, MediaMTXSettings{}, newFakeMediaMTXControl("a", "b"), newFakeMediaMTXProcess())
	now := time.Now()
	for index, id := range []string{"a", "b", "a"} {
		snapshot := Snapshot{ID: string(rune('1' + index)), JPEG: []byte{1, 2, 3, 4}, ExpiresAt: now.Add(time.Minute + time.Duration(index)*time.Second)}
		if err := server.sources[id].storeSnapshot(snapshot); err != nil {
			t.Fatal(err)
		}
	}
	if _, found := server.Snapshot("1"); found {
		t.Fatal("oldest source snapshot survived global byte budget")
	}
	for _, id := range []string{"2", "3"} {
		if _, found := server.Snapshot(id); !found {
			t.Fatal("newer bounded snapshot evicted")
		}
	}
}

func TestCameraMultipartExactPartContentLength(t *testing.T) {
	kind, wire := cameraMultipartFixture(sourceControlResponse{OK: true, JPEGBytes: 3}, []byte{1, 2, 3}, nil)
	wire = bytes.Replace(wire, []byte("Content-Type: image/jpeg"), []byte("Content-Type: image/jpeg\r\nContent-Length: 03"), 1)
	if _, _, _, err := readCameraMultipart(kind, bytes.NewReader(wire), false); err == nil {
		t.Fatal("noncanonical part length accepted")
	}
	// Unknown/legacy raw JSON framing must not be reinterpreted as capture.
	if _, _, _, err := readCameraMultipart("application/json", io.LimitReader(strings.NewReader(`{"ok":true}`), 100), false); err == nil {
		t.Fatal("raw JSON snapshot accepted")
	}
}
