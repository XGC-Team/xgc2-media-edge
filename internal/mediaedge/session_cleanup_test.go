package mediaedge

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	mtx "github.com/lxk36/xgc2-media-edge/internal/mediamtx"
)

type sessionCleanupControl struct {
	*fakeMediaMTXControl
	closeMu      sync.Mutex
	closeCalls   int
	failures     int
	listingFails bool
	entered      chan struct{}
	release      chan struct{}
}

func (control *sessionCleanupControl) CloseWHEP(ctx context.Context, location *url.URL) (bool, error) {
	control.closeMu.Lock()
	control.closeCalls++
	fail := control.failures > 0
	if fail {
		control.failures--
	}
	control.closeMu.Unlock()
	if control.entered != nil {
		control.entered <- struct{}{}
		select {
		case <-control.release:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	if fail {
		return false, errors.New("temporary WHEP deletion failure")
	}
	return control.fakeMediaMTXControl.CloseWHEP(ctx, location)
}

func (control *sessionCleanupControl) WebRTCSessions(ctx context.Context) ([]mtx.WebRTCSession, error) {
	if control.listingFails {
		return nil, errors.New("temporary session listing failure")
	}
	return control.fakeMediaMTXControl.WebRTCSessions(ctx)
}

// Seed only the established-session boundary. No camera, RTP socket or child
// process runs; the real close, HTTP and reconciliation paths own the test.
func addCleanupSession(t *testing.T, server *MediaMTXServer, control *sessionCleanupControl, id string) {
	t.Helper()
	opened, err := control.OpenWHEP(t.Context(), "camera", "offer", id)
	if err != nil {
		t.Fatal(err)
	}
	source := server.sources["camera"]
	source.sessions[id] = struct{}{}
	server.sessions[id] = &mediaMTXSession{
		id: id, upstreamID: pathBase(opened.Location.Path), location: opened.Location,
		source: source, createdAt: time.Now(),
	}
}

func TestSessionCleanupFailureRetainsOwnershipAndReconciles(t *testing.T) {
	control := &sessionCleanupControl{fakeMediaMTXControl: newFakeMediaMTXControl("camera"), failures: 1}
	server := newMediaMTXServer(Config{Sources: []SourceConfig{{ID: "camera"}}}, MediaMTXSettings{}, control, newFakeMediaMTXProcess())
	addCleanupSession(t, server, control, "revoked")
	addCleanupSession(t, server, control, "legitimate")
	handler := newHTTPServer(server)
	response := performHTTPRequest(handler, http.MethodDelete, "/api/v1/sessions/revoked", "", "127.0.0.1:4444", nil)
	if response.Code != http.StatusBadGateway {
		t.Fatalf("failed WHEP deletion returned %d, want 502", response.Code)
	}
	if len(server.sessions) != 2 || len(server.sources["camera"].sessions) != 2 || len(control.sessions) != 2 {
		t.Fatal("failed deletion dropped its session/source ownership")
	}
	// The automatic retry must also work when the independent inventory API
	// is unavailable; WHEP deletion itself is still reachable.
	control.listingFails = true
	server.reconcile()
	if len(server.sessions) != 1 || len(server.sources["camera"].sessions) != 1 || len(control.sessions) != 1 || control.closeCalls != 2 {
		t.Fatalf("retry did not converge: local=%d source=%d upstream=%d attempts=%d", len(server.sessions), len(server.sources["camera"].sessions), len(control.sessions), control.closeCalls)
	}
	if _, ok := server.sessions["legitimate"]; !ok {
		t.Fatal("revocation removed another viewer")
	}
	response = performHTTPRequest(handler, http.MethodDelete, "/api/v1/sessions/revoked", "", "127.0.0.1:4444", nil)
	if response.Code != http.StatusNotFound {
		t.Fatalf("completed cleanup should be absent: %d", response.Code)
	}
}

func TestConcurrentSessionCleanupUsesOneConfirmedDelete(t *testing.T) {
	control := &sessionCleanupControl{
		fakeMediaMTXControl: newFakeMediaMTXControl("camera"),
		entered:             make(chan struct{}, 16), release: make(chan struct{}),
	}
	server := newMediaMTXServer(Config{Sources: []SourceConfig{{ID: "camera"}}}, MediaMTXSettings{}, control, newFakeMediaMTXProcess())
	addCleanupSession(t, server, control, "revoked")
	results := make(chan error, 12)
	for range 12 {
		go func() {
			_, err := server.CloseSession("revoked")
			results <- err
		}()
	}
	select {
	case <-control.entered:
	case <-time.After(time.Second):
		t.Fatal("WHEP deletion did not start")
	}
	close(control.release)
	for range 12 {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("concurrent cleanup deadlocked")
		}
	}
	if control.closeCalls != 1 || len(server.sessions) != 0 || len(control.sessions) != 0 || len(server.sources["camera"].sessions) != 0 {
		t.Fatalf("concurrent cleanup did not converge once: calls=%d", control.closeCalls)
	}
}

func TestSessionCleanupUpstreamAbsentIsConfirmed(t *testing.T) {
	control := &sessionCleanupControl{fakeMediaMTXControl: newFakeMediaMTXControl("camera")}
	server := newMediaMTXServer(Config{Sources: []SourceConfig{{ID: "camera"}}}, MediaMTXSettings{}, control, newFakeMediaMTXProcess())
	addCleanupSession(t, server, control, "revoked")
	control.dropSession(server.sessions["revoked"].upstreamID)
	response := performHTTPRequest(newHTTPServer(server), http.MethodDelete, "/api/v1/sessions/revoked", "", "127.0.0.1:4444", nil)
	if response.Code != http.StatusNoContent || len(server.sessions) != 0 || len(server.sources["camera"].sessions) != 0 {
		t.Fatalf("already absent upstream failed to release its local ownership: %d", response.Code)
	}
}
