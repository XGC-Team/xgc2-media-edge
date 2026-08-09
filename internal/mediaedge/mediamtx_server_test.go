package mediaedge

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	mtx "github.com/lxk36/xgc2-media-edge/internal/mediamtx"
)

func TestMediaMTXServerPreservesMultiViewerSourceLease(t *testing.T) {
	capture := newCaptureControl(t)
	defer capture.close()
	rtpAddress := availableLoopbackRTPAddress(t)
	capture.setRTPDestination(t, rtpAddress)
	control := newFakeMediaMTXControl("camera")
	process := newFakeMediaMTXProcess()
	server := newMediaMTXServer(Config{
		ControlAddress: "127.0.0.1:0", SessionGracePeriod: 20 * time.Millisecond,
		SnapshotTTL: time.Second,
		Sources:     []SourceConfig{{ID: "camera", RTPListenAddress: rtpAddress, ControlSocket: capture.socket}},
	}, MediaMTXSettings{}, control, process)
	if err := server.Start(); err != nil {
		t.Fatalf("start MediaMTX server: %v", err)
	}
	defer server.Close()

	first, err := server.OpenSession(context.Background(), "camera", SessionOffer{SDP: "v=0\r\n"})
	if err != nil {
		t.Fatalf("open first session: %v", err)
	}
	second, err := server.OpenSession(context.Background(), "camera", SessionOffer{SDP: "v=0\r\n"})
	if err != nil {
		t.Fatalf("open second session: %v", err)
	}
	capture.waitFor(t, 1, activeControlRequest(true))
	if !server.CloseSession(first.SessionID) {
		t.Fatal("first session was not closed")
	}
	capture.expectNoMatch(t, 40*time.Millisecond, activeControlRequest(false))
	if !server.CloseSession(second.SessionID) {
		t.Fatal("second session was not closed")
	}
	capture.waitFor(t, 1, activeControlRequest(false))
}

func TestMediaMTXCompatibilityHTTPUsesWHEPAndReclaimsAbruptDisconnect(t *testing.T) {
	capture := newCaptureControl(t)
	defer capture.close()
	rtpAddress := availableLoopbackRTPAddress(t)
	capture.setRTPDestination(t, rtpAddress)
	control := newFakeMediaMTXControl("camera")
	server := newMediaMTXServer(Config{
		ControlAddress: "127.0.0.1:0", SessionGracePeriod: 10 * time.Millisecond,
		Sources: []SourceConfig{{ID: "camera", RTPListenAddress: rtpAddress, ControlSocket: capture.socket}},
	}, MediaMTXSettings{}, control, newFakeMediaMTXProcess())
	if err := server.Start(); err != nil {
		t.Fatalf("start MediaMTX server: %v", err)
	}
	defer server.Close()
	response, err := http.Post(
		"http://"+server.ControlAddress()+"/api/v1/sources/camera/sessions",
		"application/json", strings.NewReader(`{"sdp":"v=0\\r\\n"}`),
	)
	if err != nil {
		t.Fatalf("open compatibility session: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("compatibility session returned %s", response.Status)
	}
	var answer SessionAnswer
	if err := json.NewDecoder(response.Body).Decode(&answer); err != nil {
		t.Fatalf("decode compatibility answer: %v", err)
	}
	if answer.SDP != "v=0\r\na=recvonly\r\n" || answer.Source.ID != "camera" {
		t.Fatalf("compatibility answer = %+v", answer)
	}

	server.mu.RLock()
	item := server.sessions[answer.SessionID]
	server.mu.RUnlock()
	item.createdAt = time.Now().Add(-mediaMTXSessionAppearTimeout - time.Second)
	control.dropSession(item.upstreamID)
	server.reconcile()
	server.mu.RLock()
	_, retained := server.sessions[answer.SessionID]
	server.mu.RUnlock()
	if retained {
		t.Fatal("abruptly disconnected MediaMTX session retained its source lease")
	}
	capture.waitFor(t, 1, activeControlRequest(false))
}

func TestMediaMTXRecordingUsesNativeFMP4AndProductManifest(t *testing.T) {
	capture := newCaptureControl(t)
	defer capture.close()
	rtpAddress := availableLoopbackRTPAddress(t)
	capture.setRTPDestination(t, rtpAddress)
	control := newFakeMediaMTXControl("camera")
	recordingRoot := t.TempDir()
	config, err := (Config{
		ControlAddress: "127.0.0.1:0", SessionGracePeriod: 10 * time.Millisecond,
		Sources: []SourceConfig{{ID: "camera", RTPListenAddress: rtpAddress, ControlSocket: capture.socket}},
		Recording: RecordingConfig{
			Root: recordingRoot, MaxBitrateBitsPerSecond: 2_000_000,
			SegmentDuration: time.Second, MaxDuration: time.Minute,
			MinimumFreeBytes: 1, CapacitySafetyFactor: 1,
		},
	}).normalized()
	if err != nil {
		t.Fatalf("normalize recording config: %v", err)
	}
	server := newMediaMTXServer(config, MediaMTXSettings{}, control, newFakeMediaMTXProcess())
	if err := server.Start(); err != nil {
		t.Fatalf("start MediaMTX recording server: %v", err)
	}
	defer server.Close()
	created, err := server.StartRecording(context.Background(), "camera", StartRecordingRequest{DurationSeconds: 30})
	if err != nil {
		t.Fatalf("start native recording: %v", err)
	}
	if created.Codec.Container != "fmp4" || !created.Codec.StreamCopy {
		t.Fatalf("native recording codec = %+v", created.Codec)
	}
	stopped, err := server.StopRecording(context.Background(), created.RecordingID)
	if err != nil {
		t.Fatalf("stop native recording: %v", err)
	}
	if stopped.State != RecordingComplete || len(stopped.Segments) != 1 || stopped.Segments[0].Bytes == 0 {
		t.Fatalf("native recording manifest = %+v", stopped)
	}
	if _, err := os.Stat(filepath.Join(recordingRoot, created.RecordingID, recordingManifestName)); err != nil {
		t.Fatalf("product recording manifest missing: %v", err)
	}
}

func activeControlRequest(active bool) func(sourceControlRequest) bool {
	return func(request sourceControlRequest) bool {
		return request.Operation == "set-active" && request.Active != nil && *request.Active == active
	}
}

type fakeMediaMTXControl struct {
	mu          sync.Mutex
	paths       map[string]mtx.PathStatus
	sessions    map[string]mtx.WebRTCSession
	recordPaths map[string]string
	next        int
}

func newFakeMediaMTXControl(sourceIDs ...string) *fakeMediaMTXControl {
	paths := make(map[string]mtx.PathStatus, len(sourceIDs))
	for _, sourceID := range sourceIDs {
		paths[sourceID] = mtx.PathStatus{
			Name: sourceID, Available: true, Online: true,
			Tracks: []mtx.PathTrack{{Codec: sourceCodec}},
		}
	}
	return &fakeMediaMTXControl{
		paths: paths, sessions: make(map[string]mtx.WebRTCSession), recordPaths: make(map[string]string),
	}
}

func (control *fakeMediaMTXControl) Paths(context.Context) ([]mtx.PathStatus, error) {
	control.mu.Lock()
	defer control.mu.Unlock()
	result := make([]mtx.PathStatus, 0, len(control.paths))
	for name, path := range control.paths {
		path.InboundBytes++
		control.paths[name] = path
		result = append(result, path)
	}
	return result, nil
}

func (control *fakeMediaMTXControl) WebRTCSessions(context.Context) ([]mtx.WebRTCSession, error) {
	control.mu.Lock()
	defer control.mu.Unlock()
	result := make([]mtx.WebRTCSession, 0, len(control.sessions))
	for _, session := range control.sessions {
		result = append(result, session)
	}
	return result, nil
}

func (control *fakeMediaMTXControl) OpenWHEP(
	_ context.Context,
	name string,
	_ string,
	token string,
) (mtx.WHEPSession, error) {
	control.mu.Lock()
	defer control.mu.Unlock()
	if _, found := control.paths[name]; !found {
		return mtx.WHEPSession{}, errors.New("path not found")
	}
	control.next++
	id := "session-" + time.Unix(int64(control.next), 0).UTC().Format("150405")
	location, _ := url.Parse("http://127.0.0.1:18889/" + name + "/whep/" + id)
	control.sessions[id] = mtx.WebRTCSession{
		ID: id, Path: name, State: "read", Query: "xgcSession=" + url.QueryEscape(token),
		PeerConnectionEstablished: true,
	}
	return mtx.WHEPSession{AnswerSDP: "v=0\r\na=recvonly\r\n", Location: location}, nil
}

func (control *fakeMediaMTXControl) CloseWHEP(_ context.Context, location *url.URL) (bool, error) {
	control.mu.Lock()
	defer control.mu.Unlock()
	id := pathBase(location.Path)
	if _, found := control.sessions[id]; !found {
		return false, nil
	}
	delete(control.sessions, id)
	return true, nil
}

func (control *fakeMediaMTXControl) ConfigureRecording(
	_ context.Context,
	name string,
	settings mtx.RecordingSettings,
) error {
	control.mu.Lock()
	defer control.mu.Unlock()
	if settings.Enabled {
		control.recordPaths[name] = settings.Path
	}
	return nil
}

func (control *fakeMediaMTXControl) SetRecording(_ context.Context, name string, enabled bool) error {
	control.mu.Lock()
	defer control.mu.Unlock()
	if enabled {
		return nil
	}
	recordPath := control.recordPaths[name]
	if recordPath == "" {
		return nil
	}
	directory := filepath.Dir(recordPath)
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(directory, "segment-test.mp4"), []byte("fmp4"), 0o640)
}

func (control *fakeMediaMTXControl) dropSession(id string) {
	control.mu.Lock()
	delete(control.sessions, id)
	control.mu.Unlock()
}

type fakeMediaMTXProcess struct {
	done      chan struct{}
	closeOnce sync.Once
}

func newFakeMediaMTXProcess() *fakeMediaMTXProcess {
	return &fakeMediaMTXProcess{done: make(chan struct{})}
}

func (process *fakeMediaMTXProcess) Start(context.Context) error { return nil }
func (process *fakeMediaMTXProcess) Done() <-chan struct{}       { return process.done }
func (process *fakeMediaMTXProcess) Err() error                  { return nil }
func (process *fakeMediaMTXProcess) Close() error {
	process.closeOnce.Do(func() { close(process.done) })
	return nil
}
