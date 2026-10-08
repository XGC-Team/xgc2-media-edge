package mediaedge

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	mtx "github.com/lxk36/xgc2-media-edge/internal/mediamtx"
)

type uncertainWHEPControl struct {
	*fakeMediaMTXControl
	stateMu                    sync.Mutex
	create                     bool
	openCalls, kickCalls       int
	token                      string
	kickFailure, kickReplyLost bool
	reject                     bool
}

func (control *uncertainWHEPControl) OpenWHEP(ctx context.Context, name, offer, token string) (mtx.WHEPSession, error) {
	control.stateMu.Lock()
	control.openCalls++
	control.token = token
	create := control.create
	control.stateMu.Unlock()
	if create {
		if _, err := control.fakeMediaMTXControl.OpenWHEP(ctx, name, offer, token); err != nil {
			return mtx.WHEPSession{}, err
		}
	}
	if control.reject {
		return mtx.WHEPSession{}, &mtx.HTTPError{Operation: "WHEP POST", Status: 400, Message: "invalid offer"}
	}
	return mtx.WHEPSession{}, errors.New("WHEP creation receipt lost")
}

func (control *uncertainWHEPControl) KickWebRTCSession(ctx context.Context, id string) error {
	control.stateMu.Lock()
	control.kickCalls++
	fail, lost := control.kickFailure, control.kickReplyLost
	control.kickFailure, control.kickReplyLost = false, false
	control.stateMu.Unlock()
	if fail {
		return errors.New("native kick failed")
	}
	if err := control.fakeMediaMTXControl.KickWebRTCSession(ctx, id); err != nil {
		return err
	}
	if lost {
		return errors.New("native kick receipt lost")
	}
	return nil
}

// Begin with an established source demand boundary. Camera/XRPC completion is
// tested separately; this fixture tests real edge lease and native WHEP control.
func uncertainWHEPServer(t *testing.T, control *uncertainWHEPControl) *MediaMTXServer {
	t.Helper()
	server := newMediaMTXServer(Config{MaxSessions: 1, SessionGracePeriod: time.Hour, Sources: []SourceConfig{{ID: "camera"}}}, MediaMTXSettings{}, control, newFakeMediaMTXProcess())
	server.sources["camera"].active = true
	t.Cleanup(func() {
		server.mu.Lock()
		server.closing = true
		server.cancelLifecycle()
		server.mu.Unlock()
		server.background.Wait()
		source := server.sources["camera"]
		source.mu.Lock()
		source.cancelDeactivateTimerLocked()
		source.mu.Unlock()
		if source.controlRPC != nil {
			source.controlRPC.close()
		}
	})
	return server
}

func TestWHEPCreationReceiptLossHoldsSlotUntilNativeCleanup(t *testing.T) {
	for _, failure := range []string{"none", "kick-failure", "kick-reply-lost"} {
		t.Run(failure, func(t *testing.T) {
			control := &uncertainWHEPControl{fakeMediaMTXControl: newFakeMediaMTXControl("camera"), create: true, kickFailure: failure == "kick-failure", kickReplyLost: failure == "kick-reply-lost"}
			server := uncertainWHEPServer(t, control)
			if _, err := server.OpenSession(t.Context(), "camera", SessionOffer{SDP: "offer"}); err == nil {
				t.Fatal("lost receipt reported success")
			}
			if len(server.sessions) != 1 || len(server.sources["camera"].sessions) != 1 || server.pendingSessions != 0 {
				t.Fatal("uncertain native creation lost its lease or capacity reservation")
			}
			if _, err := server.OpenSession(t.Context(), "camera", SessionOffer{SDP: "offer"}); !errors.Is(err, ErrMediaCapacity) {
				t.Fatalf("uncertain slot was reused: %v", err)
			}
			server.reconcile()
			if failure != "none" && len(server.sessions) != 1 {
				t.Fatal("failed cleanup freed source ownership")
			}
			server.reconcile()
			if len(server.sessions) != 0 || len(server.sources["camera"].sessions) != 0 || len(control.sessions) != 0 || control.openCalls != 1 {
				t.Fatal("native cleanup did not converge without replaying creation")
			}
			if failure == "kick-reply-lost" && control.kickCalls != 1 {
				t.Fatal("confirmed inventory absence unnecessarily repeated kick")
			}
		})
	}
}

func TestWHEPUnknownCreationAbsenceRetainsUntilObservedOrChildExit(t *testing.T) {
	control := &uncertainWHEPControl{fakeMediaMTXControl: newFakeMediaMTXControl("camera")}
	server := uncertainWHEPServer(t, control)
	if _, err := server.OpenSession(t.Context(), "camera", SessionOffer{SDP: "offer"}); err == nil {
		t.Fatal("lost receipt reported success")
	}
	server.reconcile()
	if len(server.sessions) != 1 || len(server.sources["camera"].sessions) != 1 || control.kickCalls != 0 {
		t.Fatal("empty inventory was mistaken for cancelled native creation")
	}
	// The native job can finish after the caller has gone. Its preserved token
	// permits cleanup without making another POST or touching another viewer.
	if _, err := control.fakeMediaMTXControl.OpenWHEP(t.Context(), "camera", "offer", control.token); err != nil {
		t.Fatal(err)
	}
	server.reconcile()
	if len(server.sessions) != 0 || len(control.sessions) != 0 || control.openCalls != 1 {
		t.Fatal("delayed creation was leaked or replayed")
	}
}

func TestWHEPUnknownCreationRejectsWrongIdentityAndDuplicateToken(t *testing.T) {
	for _, mutation := range []string{"wrong-path", "wrong-state", "duplicate-token"} {
		t.Run(mutation, func(t *testing.T) {
			control := &uncertainWHEPControl{fakeMediaMTXControl: newFakeMediaMTXControl("camera"), create: true}
			server := uncertainWHEPServer(t, control)
			_, _ = server.OpenSession(t.Context(), "camera", SessionOffer{SDP: "offer"})
			control.mu.Lock()
			for id, actual := range control.sessions {
				switch mutation {
				case "wrong-path":
					actual.Path = "another-camera"
				case "wrong-state":
					actual.State = "publish"
				case "duplicate-token":
					duplicate := actual
					duplicate.ID = "other-session"
					duplicate.Query = "xgcSession=" + url.QueryEscape(control.token)
					control.sessions[duplicate.ID] = duplicate
				}
				control.sessions[id] = actual
				break
			}
			control.mu.Unlock()
			server.reconcile()
			if len(server.sessions) != 1 || control.kickCalls != 0 {
				t.Fatal("unverified inventory caused a destructive native kick or freed lease")
			}
		})
	}
}

func TestWHEPReconciliationCleanupAttemptsAreBoundedAndFair(t *testing.T) {
	control := &uncertainWHEPControl{fakeMediaMTXControl: newFakeMediaMTXControl("camera")}
	server := uncertainWHEPServer(t, control)
	for index := range 12 {
		id := fmt.Sprintf("unknown-%02d", index)
		_, _ = control.fakeMediaMTXControl.OpenWHEP(t.Context(), "camera", "offer", id)
		server.sessions[id] = &mediaMTXSession{id: id, source: server.sources["camera"], closeRequested: true}
		server.sources["camera"].sessions[id] = struct{}{}
	}
	server.reconcile()
	if control.kickCalls != mediaMTXReconcileCloseLimit || len(server.sessions) != 8 {
		t.Fatal("reconciliation exceeded its per-pass native cleanup budget")
	}
	server.reconcile()
	server.reconcile()
	if control.kickCalls != 12 || len(server.sessions) != 0 {
		t.Fatal("bounded reconciliation starved another unknown session")
	}
}

func TestWHEPCompletedNativeRejectionReleasesAfterCompleteInventory(t *testing.T) {
	control := &uncertainWHEPControl{fakeMediaMTXControl: newFakeMediaMTXControl("camera"), reject: true}
	server := uncertainWHEPServer(t, control)
	if _, err := server.OpenSession(t.Context(), "camera", SessionOffer{SDP: "offer"}); err == nil {
		t.Fatal("native rejection reported creation")
	}
	if len(server.sessions) != 1 {
		t.Fatal("rejection skipped authoritative native inventory")
	}
	server.reconcile()
	if len(server.sessions) != 0 || control.kickCalls != 0 || control.openCalls != 1 {
		t.Fatal("completed native rejection leaked capacity or replayed POST")
	}
}

func TestWHEPUnobservedCreationReleasesOnlyAfterManagedChildExit(t *testing.T) {
	probe := newLifecycleControlProbe(t)
	control := &uncertainWHEPControl{fakeMediaMTXControl: newFakeMediaMTXControl("camera")}
	server := uncertainWHEPServer(t, control)
	source := server.sources["camera"]
	source.config.ControlSocket, source.config.ControlInstanceID = probe.socket, "test-instance"
	if _, err := server.OpenSession(t.Context(), "camera", SessionOffer{SDP: "offer"}); err == nil {
		t.Fatal("lost receipt reported success")
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if len(server.sessions) != 0 || len(source.sessions) != 0 || !server.closeComplete || probe.stops.Load() != 1 || control.openCalls != 1 {
		t.Fatal("confirmed child/native stop did not release unknown creation without replay")
	}
}
