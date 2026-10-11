package mediaedge

import (
	"context"
	"encoding/json"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/httpx"
	"golang.org/x/sys/unix"
)

type restartingCamera struct {
	server    *http.Server
	done      chan struct{}
	starts    atomic.Int32
	stops     atomic.Int32
	active    atomic.Bool
	loseStart atomic.Bool
}

func startRestartingCamera(t *testing.T, socket, address, instance string) *restartingCamera {
	t.Helper()
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	camera := &restartingCamera{done: make(chan struct{})}
	description := defaultCaptureDescription()
	_, portText, _ := net.SplitHostPort(address)
	description.RTPPort, _ = strconv.Atoi(portText)
	ref := xrpc.ServiceRef{TargetID: "peer-test", Service: "camera-source", APIVersion: "v1", InstanceID: instance,
		Profile: xrpc.HTTP, Endpoint: xrpc.Endpoint{Kind: "unix", Address: socket}}
	camera.server = &http.Server{ReadHeaderTimeout: time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Xrpc-Instance-ID", instance)
		if r.URL.Path == "/v1/describe" {
			if r.Header.Get("X-Xrpc-Instance-ID") != "" || r.URL.Query().Get("wait_ready_ms") != "30000" {
				t.Error("readiness discovery must be unbound and held when not ready")
			}
			_ = json.NewEncoder(w).Encode(cameraDiscovery(ref))
			return
		}
		if r.Header.Get("X-Xrpc-Instance-ID") != instance {
			w.WriteHeader(http.StatusConflict)
			return
		}
		switch pathBase(r.URL.Path) {
		case "describe":
			_ = json.NewEncoder(w).Encode(description)
		case "start", "stop", "status", "request-keyframe":
			switch pathBase(r.URL.Path) {
			case "start":
				camera.starts.Add(1)
				camera.active.Store(true)
				if camera.loseStart.Swap(false) {
					connection, _, _ := w.(http.Hijacker).Hijack()
					_ = connection.Close()
					return
				}
			case "stop":
				camera.stops.Add(1)
				camera.active.Store(false)
			}
			state := "idle"
			if camera.active.Load() {
				state = "active"
			}
			_ = json.NewEncoder(w).Encode(sourceControlResponse{OK: true, ManagedSourceID: "camera", Active: camera.active.Load(),
				AppliedActive: camera.active.Load(), State: state, Completion: "applied", ConfigurationRevision: 1})
		case "capture":
			var request sourceControlRequest
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			rgb := make([]byte, 16*16*3)
			if request.IncludeRGB != nil && !*request.IncludeRGB {
				rgb = nil
			}
			jpeg := []byte("\xff\xd8xgc\xff\xd9")
			metadata := sourceControlResponse{OK: true, SourceID: "camera", SnapshotID: request.SnapshotID,
				FrameID: "camera_optical", Sequence: 1, Width: 16, Height: 16, PixelFormat: "rgb8",
				TimestampNanoseconds: 123456789, TimestampClockDomain: "device", JPEGBytes: len(jpeg), RGBBytes: len(rgb)}
			kind, body := cameraMultipartFixture(metadata, jpeg, rgb)
			w.Header().Set("Content-Type", kind)
			_, _ = w.Write(body)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})}
	go func() { defer close(camera.done); _ = camera.server.Serve(listener) }()
	t.Cleanup(camera.close)
	return camera
}

func (camera *restartingCamera) close() { _ = camera.server.Close(); <-camera.done }

func cameraPeerServer(t *testing.T) (*MediaMTXServer, string, string, *lockedBuffer) {
	t.Helper()
	directory, err := os.MkdirTemp("", "camera-peer-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	socket, address := filepath.Join(directory, "camera.sock"), availableLoopbackRTPAddress(t)
	config, err := (Config{TargetID: "peer-test", ControlAddress: "127.0.0.1:0", RPCSocket: filepath.Join(directory, "edge.sock"),
		SessionGracePeriod: 20 * time.Millisecond, Sources: []SourceConfig{{ID: "camera", ControlSocket: socket, RTPListenAddress: address}}}).normalized()
	if err != nil {
		t.Fatal(err)
	}
	logs := &lockedBuffer{}
	edge := newMediaMTXServer(config, MediaMTXSettings{Stderr: logs}, newFakeMediaMTXControl("camera"), newFakeMediaMTXProcess())
	if err := edge.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := edge.Close(); err != nil {
			t.Error(err)
		}
	})
	peer := edge.sources["camera"].controller().(*cameraPeer)
	peer.rng = rand.New(rand.NewSource(2))
	return edge, socket, address, logs
}

func TestCameraPeerStartsLateAndReleasesLeaseToNewInstance(t *testing.T) {
	edge, socket, address, _ := cameraPeerServer(t)
	defer func() {
		if err := edge.Close(); err != nil {
			t.Error(err)
		}
	}()
	peer := edge.sources["camera"].controller().(*cameraPeer)
	time.Sleep(30 * time.Millisecond)
	peer.mu.Lock()
	if peer.attempts != 0 {
		t.Fatal("camera discovery was eager")
	}
	peer.mu.Unlock()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	client, err := httpx.New(httpx.Config{LocalTargetID: "peer-test", Service: xrpc.ServiceRef{TargetID: "peer-test",
		Service: "media-edge", APIVersion: "v1", Profile: xrpc.HTTP, Endpoint: xrpc.Endpoint{Kind: "unix", Address: edge.config.RPCSocket}}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	data, status, _, err := client.Do(ctx, http.MethodGet, "/v1/describe?wait_ready_ms=30000", "ready-without-camera", "", nil)
	var description struct {
		Ready bool
		Facts struct{ Sources []SourceStatus }
	}
	if err != nil || status != 200 || json.Unmarshal(data, &description) != nil || !description.Ready ||
		len(description.Facts.Sources) != 1 || description.Facts.Sources[0].ControlHealthy {
		t.Fatalf("edge readiness depended on its camera: status=%d err=%v body=%s", status, err, data)
	}
	started := time.Now()
	if _, err := edge.OpenSession(t.Context(), "camera", SessionOffer{SDP: "offer"}); err == nil {
		t.Fatal("missing camera accepted a viewer")
	}
	if time.Since(started) > time.Second {
		t.Fatal("missing camera blocked a viewer")
	}
	first := startRestartingCamera(t, socket, address, "first-camera")
	eventually(t, 3*time.Second, func() bool { return edge.SourceStatuses()[0].ControlHealthy })
	answer, err := edge.OpenSession(t.Context(), "camera", SessionOffer{SDP: "offer"})
	if err != nil {
		t.Fatal(err)
	}
	first.close()
	eventually(t, time.Second, func() bool { return !edge.SourceStatuses()[0].ControlHealthy })
	started = time.Now()
	if _, err := edge.OpenSession(t.Context(), "camera", SessionOffer{SDP: "offer"}); err == nil {
		t.Fatal("down camera accepted a viewer")
	}
	if time.Since(started) > 100*time.Millisecond {
		t.Fatal("down camera did not fail promptly")
	}
	second := startRestartingCamera(t, socket, address, "second-camera")
	eventually(t, 3*time.Second, func() bool {
		status := edge.SourceStatuses()[0]
		return status.ControlInstanceID == "second-camera" && status.AppliedLeaseRevision == status.DesiredLeaseRevision && second.starts.Load() == 1
	})
	time.Sleep(100 * time.Millisecond)
	if second.starts.Load() != 1 {
		t.Fatal("lease was replayed repeatedly")
	}
	if _, err := edge.CloseSession(answer.SessionID); err != nil {
		t.Fatal(err)
	}
	eventually(t, time.Second, func() bool { return second.stops.Load() == 1 })
	second.close()
	third := startRestartingCamera(t, socket, address, "third-camera")
	eventually(t, 3*time.Second, func() bool { return third.stops.Load() == 1 })
	if third.starts.Load() != 0 {
		t.Fatal("a replacement resurrected a stopped lease")
	}
}

func TestCameraPeerConfirmsLostLeaseReceiptWithoutResending(t *testing.T) {
	edge, socket, address, _ := cameraPeerServer(t)
	defer func() {
		if err := edge.Close(); err != nil {
			t.Error(err)
		}
	}()
	camera := startRestartingCamera(t, socket, address, "lost-receipt")
	camera.loseStart.Store(true)
	source := edge.sources["camera"]
	source.mu.Lock()
	source.sessions["viewer"] = struct{}{}
	source.mu.Unlock()
	if err := source.acquire(t.Context()); err == nil {
		t.Fatal("lost native receipt was reported applied")
	}
	eventually(t, 3*time.Second, func() bool {
		status := edge.SourceStatuses()[0]
		return status.ControlHealthy && status.DesiredLeaseRevision == status.AppliedLeaseRevision
	})
	if camera.starts.Load() != 1 {
		t.Fatal("a lost receipt caused a second write to the same revision")
	}
	source.removeSession("viewer")
	eventually(t, time.Second, func() bool { return camera.stops.Load() == 1 })
}

func TestCameraPeerCaptureRecoversAfterSocketReplacement(t *testing.T) {
	edge, socket, address, _ := cameraPeerServer(t)
	defer func() {
		if err := edge.Close(); err != nil {
			t.Error(err)
		}
	}()
	first := startRestartingCamera(t, socket, address, "capture-first")
	if _, err := edge.CaptureSnapshot(t.Context(), "camera", SnapshotCaptureRequest{}); err != nil {
		t.Fatal(err)
	}
	first.close()
	eventually(t, time.Second, func() bool { return !edge.SourceStatuses()[0].ControlHealthy })
	started := time.Now()
	if _, err := edge.CaptureSnapshot(t.Context(), "camera", SnapshotCaptureRequest{}); err == nil {
		t.Fatal("down source accepted capture")
	}
	if time.Since(started) > 100*time.Millisecond {
		t.Fatal("down source blocked capture")
	}
	startRestartingCamera(t, socket, address, "capture-second")
	eventually(t, 3*time.Second, func() bool {
		return edge.SourceStatuses()[0].ControlInstanceID == "capture-second" && edge.SourceStatuses()[0].ControlHealthy
	})
	if snapshot, err := edge.CaptureSnapshot(t.Context(), "camera", SnapshotCaptureRequest{}); err != nil || len(snapshot.RGB) != 16*16*3 {
		t.Fatalf("replacement capture: RGB=%d err=%v", len(snapshot.RGB), err)
	}
}

func peerProcessResources(t *testing.T) (float64, int, int) {
	t.Helper()
	var usage unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_SELF, &usage); err != nil {
		t.Fatal(err)
	}
	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	threads, err := os.ReadDir("/proc/self/task")
	if err != nil {
		t.Fatal(err)
	}
	cpu := float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6
	return cpu, len(fds), len(threads)
}

// Run explicitly with XGC_MEDIA_LONG_TESTS=1 go test -p 2 ./internal/mediaedge
// -run TestCameraPeerDown60Seconds -count=1. Fast CI skips the wall-clock soak.
func TestCameraPeerDown60Seconds(t *testing.T) {
	if os.Getenv("XGC_MEDIA_LONG_TESTS") != "1" {
		t.Skip("set XGC_MEDIA_LONG_TESTS=1 for the 60-second peer-down soak")
	}
	edge, socket, address, logs := cameraPeerServer(t)
	defer func() {
		if err := edge.Close(); err != nil {
			t.Error(err)
		}
	}()
	first := startRestartingCamera(t, socket, address, "before-outage")
	answer, err := edge.OpenSession(t.Context(), "camera", SessionOffer{SDP: "offer"})
	if err != nil {
		t.Fatal(err)
	}
	// Preserve a native viewer across the whole outage in the fake media kernel.
	edge.control.(*fakeMediaMTXControl).mu.Lock()
	for id, session := range edge.control.(*fakeMediaMTXControl).sessions {
		session.PeerConnectionEstablished = true
		edge.control.(*fakeMediaMTXControl).sessions[id] = session
	}
	edge.control.(*fakeMediaMTXControl).mu.Unlock()
	time.Sleep(100 * time.Millisecond)
	_, beforeFDs, beforeThreads := peerProcessResources(t)
	first.close()
	eventually(t, time.Second, func() bool { return !edge.SourceStatuses()[0].ControlHealthy })
	peer := edge.sources["camera"].controller().(*cameraPeer)
	peer.mu.Lock()
	beforeAttempts := peer.attempts
	peer.mu.Unlock()
	beforeCPU, _, _ := peerProcessResources(t)
	beforeLines := strings.Count(logs.String(), "\n")
	time.Sleep(60 * time.Second)
	afterCPU, _, downThreads := peerProcessResources(t)
	peer.mu.Lock()
	attempts := peer.attempts - beforeAttempts
	peer.mu.Unlock()
	lines := strings.Count(logs.String(), "\n") - beforeLines
	if afterCPU-beforeCPU >= 1.2 || attempts > 15 || lines > 7 || downThreads > beforeThreads+1 {
		t.Fatalf("outage was not idle: CPU=%.3fs attempts=%d lines=%d threads=%d/%d", afterCPU-beforeCPU, attempts, lines, beforeThreads, downThreads)
	}
	second := startRestartingCamera(t, socket, address, "after-outage")
	eventually(t, 3*time.Second, func() bool { return second.starts.Load() == 1 && edge.SourceStatuses()[0].ControlHealthy })
	time.Sleep(200 * time.Millisecond)
	_, afterFDs, afterThreads := peerProcessResources(t)
	if second.starts.Load() != 1 || afterFDs > beforeFDs || afterThreads > beforeThreads+1 {
		t.Fatalf("reconnect leaked or replayed: starts=%d fds=%d/%d threads=%d/%d", second.starts.Load(), beforeFDs, afterFDs, beforeThreads, afterThreads)
	}
	t.Logf("60s outage: CPU=%.3fs attempts=%d lines=%d fds=%d->%d threads=%d->%d", afterCPU-beforeCPU, attempts, lines, beforeFDs, afterFDs, beforeThreads, afterThreads)
	if _, err := edge.CloseSession(answer.SessionID); err != nil {
		t.Fatal(err)
	}
}
