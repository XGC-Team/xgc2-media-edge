package mediaedge

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	mtx "github.com/lxk36/xgc2-media-edge/internal/mediamtx"
)

// The probe uses actual Unix transactions. Dropping a reply models an
// ambiguous acknowledgment: the adapter may already have applied the command.
type lifecycleControlProbe struct {
	socket     string
	starts     atomic.Int32
	stops      atomic.Int32
	failStarts atomic.Int32
	failStops  atomic.Int32
}

func newLifecycleControlProbe(t *testing.T) *lifecycleControlProbe {
	t.Helper()
	directory, err := os.MkdirTemp("", "mtx-lifecycle-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	probe := &lifecycleControlProbe{socket: filepath.Join(directory, "source.sock")}
	listener, err := net.Listen("unix", probe.socket)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			func() {
				defer connection.Close()
				_ = connection.SetDeadline(time.Now().Add(time.Second))
				var request sourceControlRequest
				if json.NewDecoder(connection).Decode(&request) != nil {
					return
				}
				if request.Operation == "set-active" && request.Active != nil {
					if *request.Active {
						if probe.starts.Add(1) <= probe.failStarts.Load() {
							return
						}
					} else if probe.stops.Add(1) <= probe.failStops.Load() {
						return
					}
				}
				_ = json.NewEncoder(connection).Encode(map[string]bool{"ok": true})
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("control probe did not exit")
		}
	})
	return probe
}

func newLifecycleSource(t *testing.T, probe *lifecycleControlProbe) *mediaMTXSource {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	server := &MediaMTXServer{
		config: Config{SessionGracePeriod: time.Hour}, lifecycleContext: ctx,
	}
	source := &mediaMTXSource{
		server: server, config: SourceConfig{ID: "camera", ControlSocket: probe.socket},
		sessions: make(map[string]struct{}), active: true,
		activeSince: time.Now().Add(-10 * sourceStallTimeout),
	}
	t.Cleanup(func() {
		server.mu.Lock()
		server.closing = true
		cancel()
		server.mu.Unlock()
		source.lifecycleMu.Lock()
		source.mu.Lock()
		source.cancelDeactivateTimerLocked()
		source.mu.Unlock()
		source.lifecycleMu.Unlock()
	})
	return source
}

func waitLifecycleCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for !condition() {
		select {
		case <-deadline.C:
			t.Fatal("source lifecycle did not converge")
		case <-ticker.C:
		}
	}
}

func TestMediaMTXFailedDeactivateRetriesUntilAcknowledged(t *testing.T) {
	probe := newLifecycleControlProbe(t)
	probe.failStops.Store(1)
	source := newLifecycleSource(t, probe)
	source.deactivateIfUnused(source.deactivateEpoch)
	source.mu.Lock()
	active, uncertain, retry := source.active, source.deactivateUncertain, source.deactivateTimer != nil
	source.mu.Unlock()
	if !active || !uncertain || !retry {
		t.Fatalf("failed stop: active=%t uncertain=%t retry=%t", active, uncertain, retry)
	}
	waitLifecycleCondition(t, func() bool {
		source.mu.Lock()
		defer source.mu.Unlock()
		return !source.active && !source.deactivateUncertain && source.deactivateTimer == nil
	})
	if got := probe.stops.Load(); got != 2 {
		t.Fatalf("stop attempts=%d, want 2", got)
	}
}

func TestMediaMTXAcquireConfirmsActivationAfterLostStopReply(t *testing.T) {
	probe := newLifecycleControlProbe(t)
	probe.failStops.Store(1)
	source := newLifecycleSource(t, probe)
	epoch := source.deactivateEpoch
	source.deactivateIfUnused(epoch)
	if err := source.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	source.releasePending("new-viewer")
	// An already-fired retry must also be harmless after reacquisition.
	source.deactivateIfUnused(epoch)
	source.mu.Lock()
	active, uncertain, retry := source.active, source.deactivateUncertain, source.deactivateTimer != nil
	source.mu.Unlock()
	if !active || uncertain || retry || probe.starts.Load() != 1 || probe.stops.Load() != 1 {
		t.Fatalf("reacquire: active=%t uncertain=%t retry=%t starts=%d stops=%d", active, uncertain, retry, probe.starts.Load(), probe.stops.Load())
	}
}

func TestMediaMTXFailedReacquireRetainsIdleCleanup(t *testing.T) {
	probe := newLifecycleControlProbe(t)
	probe.failStops.Store(1)
	probe.failStarts.Store(1)
	source := newLifecycleSource(t, probe)
	source.deactivateIfUnused(source.deactivateEpoch)
	if err := source.acquire(context.Background()); err == nil {
		t.Fatal("reacquire with a lost activation reply unexpectedly succeeded")
	}
	source.mu.Lock()
	defer source.mu.Unlock()
	if source.pending != 0 || !source.active || !source.deactivateUncertain || source.deactivateTimer == nil {
		t.Fatal("failed reacquire lost its idle cleanup intent")
	}
}

func TestMediaMTXRecoveryCannotReactivateStoppedSource(t *testing.T) {
	probe := newLifecycleControlProbe(t)
	source := newLifecycleSource(t, probe)
	epoch := source.deactivateEpoch
	source.recoveryPending = true
	source.deactivateIfUnused(epoch)
	source.recover(epoch)
	if got := probe.starts.Load(); got != 0 {
		t.Fatalf("stale recovery activated stopped source %d times", got)
	}
}

func TestMediaMTXRecoveryRejectsReplacedDemandGeneration(t *testing.T) {
	probe := newLifecycleControlProbe(t)
	source := newLifecycleSource(t, probe)
	source.sessions["viewer"] = struct{}{}
	epoch := source.deactivateEpoch
	source.recoveryPending = true
	if err := source.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	source.releasePending("")
	source.recover(epoch)
	if got := probe.starts.Load(); got != 0 {
		t.Fatalf("old generation issued %d activations", got)
	}
}

func TestMediaMTXRecoveryRequiresCurrentDemand(t *testing.T) {
	for _, consumer := range []string{"none", "viewer", "recording"} {
		t.Run(consumer, func(t *testing.T) {
			probe := newLifecycleControlProbe(t)
			source := newLifecycleSource(t, probe)
			if consumer == "viewer" {
				source.sessions["viewer"] = struct{}{}
			} else if consumer == "recording" {
				source.recordingID = "recording"
			}
			source.recoveryPending = true
			source.recover(source.deactivateEpoch)
			want := int32(1)
			if consumer == "none" {
				want = 0
			}
			if got := probe.starts.Load(); got != want {
				t.Fatalf("activation count=%d, want %d", got, want)
			}
			source.mu.Lock()
			pending := source.recoveryPending
			source.mu.Unlock()
			if pending {
				t.Fatal("completed recovery retained single-flight ownership")
			}
		})
	}
}

func TestMediaMTXRecoveryCoalescesWhileLifecycleIsBusy(t *testing.T) {
	probe := newLifecycleControlProbe(t)
	source := newLifecycleSource(t, probe)
	source.sessions["viewer"] = struct{}{}
	now := time.Now()
	func() {
		source.lifecycleMu.Lock()
		defer source.lifecycleMu.Unlock()
		source.observePath(now, mtx.PathStatus{})
		source.observePath(now.Add(2*sourceRecoveryMinimumInterval), mtx.PathStatus{})
		source.mu.Lock()
		pending, attempted := source.recoveryPending, source.lastRecoveryAttemptAt
		source.mu.Unlock()
		if !pending || !attempted.Equal(now) {
			t.Errorf("queued recovery was duplicated: pending=%t attempted=%v want=%v", pending, attempted, now)
		}
	}()
	waitLifecycleCondition(t, func() bool {
		source.mu.Lock()
		defer source.mu.Unlock()
		return !source.recoveryPending && probe.starts.Load() == 1
	})
}
