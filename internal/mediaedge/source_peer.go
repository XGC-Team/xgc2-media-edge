package mediaedge

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
)

var ErrSourceUnavailable = errors.New("camera source is unavailable")

// cameraPeer owns one lazy reconnect flight and the latest desired lease. The
// transport never reconnects from a viewer, capture, or status request.
type cameraPeer struct {
	control  *cameraControl
	source   *mediaMTXSource
	expected SourceConfig
	once     sync.Once
	mu       sync.Mutex
	wake     chan struct{}
	changed  chan struct{}
	first    chan struct{}

	connected       bool
	invalidated     bool
	cancelAttempt   context.CancelFunc
	desiredActive   bool
	desiredRevision uint64
	appliedRevision uint64
	appliedInstance string
	sentInstance    string
	sentRevision    uint64
	receipt         sourceControlResponse
	lastError       error
	attempts        uint64
	rng             *rand.Rand
}

func newCameraPeer(source *mediaMTXSource, control *cameraControl) *cameraPeer {
	control.waitReady = true
	control.clientConfig.MaxCallTime = 31 * time.Second
	control.clientConfig.HeaderTimeout = 31 * time.Second
	return &cameraPeer{source: source, control: control, expected: source.config, wake: make(chan struct{}, 1),
		changed: make(chan struct{}), first: make(chan struct{}),
		rng: rand.New(rand.NewSource(rand.Int63()))}
}

func (peer *cameraPeer) start() {
	peer.once.Do(func() {
		if !peer.source.server.startBackground(peer.run) {
			close(peer.first)
		}
	})
}

func (peer *cameraPeer) signal() {
	select {
	case peer.wake <- struct{}{}:
	default:
	}
}

func (peer *cameraPeer) notifyLocked() {
	close(peer.changed)
	peer.changed = make(chan struct{})
}

// A socket event or failed fenced call retires the old transport. Notifications
// coalesce; the worker alone discovers, validates and applies the next lease.
func (peer *cameraPeer) invalidate() {
	peer.mu.Lock()
	peer.connected = false
	peer.invalidated = true
	if peer.cancelAttempt != nil {
		peer.cancelAttempt()
	}
	peer.notifyLocked()
	peer.mu.Unlock()
	peer.observe(sourceControlResponse{}, ErrSourceUnavailable)
	peer.signal()
}

func (peer *cameraPeer) boundInstance() string { return peer.control.boundInstance() }

func (peer *cameraPeer) boundReference() (xrpc.ServiceRef, bool) {
	peer.start()
	select {
	case <-peer.first:
	case <-time.After(sourceControlRequestTimeout):
		return xrpc.ServiceRef{}, false
	}
	peer.mu.Lock()
	connected := peer.connected
	peer.mu.Unlock()
	if !connected {
		return xrpc.ServiceRef{}, false
	}
	return peer.control.boundReference()
}

func (peer *cameraPeer) close() { peer.control.close() }

func (peer *cameraPeer) call(ctx context.Context, request sourceControlRequest) (sourceControlResponse, []byte, []byte, error) {
	if peer.source.server.lifecycleContext.Err() != nil {
		// Shutdown drains the worker before confirming the native stop. This
		// finite cleanup call uses the last fenced transport and is not a retry
		// worker or a replay to a replacement process.
		return peer.control.call(ctx, request)
	}
	lease := request.Operation == "start" || request.Operation == "stop"
	peer.mu.Lock()
	if lease && (peer.desiredRevision == 0 || peer.desiredActive != (request.Operation == "start")) {
		peer.desiredRevision++
		peer.desiredActive = request.Operation == "start"
	}
	revision := peer.desiredRevision
	peer.mu.Unlock()
	peer.start()
	peer.signal()
	// Only the first caller waits for the initial discovery attempt. During an
	// outage subsequent callers receive an immediate unavailable response.
	timeout := sourceControlRequestTimeout
	if request.Operation == "capture" {
		timeout = sourceSnapshotRequestTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	select {
	case <-peer.first:
	case <-ctx.Done():
		return sourceControlResponse{}, nil, nil, ErrSourceUnavailable
	}
	for {
		peer.mu.Lock()
		connected, changed := peer.connected, peer.changed
		applied, receipt, lastError := peer.appliedRevision, peer.receipt, peer.lastError
		peer.mu.Unlock()
		if !connected {
			return sourceControlResponse{}, nil, nil, fmt.Errorf("%w: %v", ErrSourceUnavailable, lastError)
		}
		if !lease {
			response, jpeg, rgb, err := peer.control.callBound(ctx, request)
			var received *sourceControlHTTPError
			if err != nil && (!errors.As(err, &received) || received.status == 503) {
				peer.invalidate()
			}
			return response, jpeg, rgb, err
		}
		if applied >= revision {
			return receipt, nil, nil, nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return sourceControlResponse{}, nil, nil, fmt.Errorf("%w: lease not yet applied", ErrSourceUnavailable)
		}
	}
}

func (peer *cameraPeer) observe(response sourceControlResponse, err error) {
	source := peer.source
	source.mu.Lock()
	defer source.mu.Unlock()
	source.controlObservedAt = time.Now().UTC()
	source.controlHealthy = err == nil
	source.controlError = ""
	if err != nil {
		source.controlError = err.Error()
		if len(source.controlError) > 512 {
			source.controlError = source.controlError[:512]
		}
		source.available, source.online = false, false
	} else {
		source.nativeStatus = response
		if response.Completion == "applied" {
			source.deactivateUncertain = false
		}
	}
}

func (peer *cameraPeer) connect(ctx context.Context) error {
	if _, err := peer.control.transport(ctx); err != nil {
		return err
	}
	described, err := describeSourceWith(ctx, peer.expected, peer.control)
	if err != nil {
		return err
	}
	source := peer.source
	source.mu.Lock()
	source.config.Width, source.config.Height = described.Width, described.Height
	source.config.FPS, source.config.FrameID = described.FPS, described.FrameID
	source.config.CaptureSupported = described.CaptureSupported
	source.config.KeyframeRequestSupported = described.KeyframeRequestSupported
	source.mu.Unlock()
	response, _, _, err := peer.control.call(ctx, sourceControlRequest{Operation: "status"})
	if err == nil && response.ManagedSourceID != source.config.ID {
		err = errors.New("source status identity mismatch")
	}
	if err == nil {
		peer.mu.Lock()
		// Resolve a lost lifecycle receipt from native applied state before
		// sending another write to the same incarnation and revision.
		if peer.sentInstance == peer.control.boundInstance() && peer.sentRevision == peer.desiredRevision &&
			peer.desiredRevision != 0 && response.AppliedActive == peer.desiredActive &&
			((peer.desiredActive && response.State == "active") || (!peer.desiredActive && response.State == "idle")) {
			response.Active, response.Completion = response.AppliedActive, "applied"
			peer.appliedInstance = peer.sentInstance
			peer.appliedRevision = peer.sentRevision
			peer.receipt = response
		}
		peer.mu.Unlock()
		peer.observe(response, nil)
	}
	return err
}

func (peer *cameraPeer) apply(ctx context.Context) error {
	peer.mu.Lock()
	revision, active := peer.desiredRevision, peer.desiredActive
	instance := peer.control.boundInstance()
	alreadyApplied := peer.appliedInstance == instance && peer.appliedRevision == revision
	peer.mu.Unlock()
	if revision == 0 || alreadyApplied {
		return nil
	}
	operation := "stop"
	if active {
		operation = "start"
	}
	peer.mu.Lock()
	peer.sentInstance, peer.sentRevision = instance, revision
	peer.mu.Unlock()
	response, _, _, err := peer.control.call(ctx, sourceControlRequest{Operation: operation})
	if err != nil {
		return err
	}
	peer.mu.Lock()
	peer.appliedInstance, peer.appliedRevision, peer.receipt = instance, revision, response
	peer.notifyLocked()
	peer.mu.Unlock()
	response.DesiredActive, response.AppliedActive = active, active
	peer.observe(response, nil)
	return nil
}

func (peer *cameraPeer) run() {
	ctx := peer.source.server.lifecycleContext
	watcher, err := peer.source.server.watchSourceSockets()
	if err == nil {
		err = watcher.add(peer)
	}
	if err != nil {
		peer.observe(sourceControlResponse{}, err)
		close(peer.first)
		return
	}
	defer watcher.remove(peer)
	first := true
	backoff := 250 * time.Millisecond
	var lastLog time.Time
	suppressed := 0
	for ctx.Err() == nil {
		peer.mu.Lock()
		invalidated, connected := peer.invalidated, peer.connected
		peer.invalidated = false
		peer.mu.Unlock()
		if invalidated {
			peer.control.close()
			connected = false
		}
		attemptContext, cancel := context.WithTimeout(ctx, 31*time.Second)
		peer.mu.Lock()
		peer.cancelAttempt = cancel
		if !connected {
			peer.attempts++
		}
		peer.mu.Unlock()
		if !connected {
			err = peer.connect(attemptContext)
		}
		if err == nil {
			err = peer.apply(attemptContext)
		}
		cancel()
		peer.mu.Lock()
		peer.cancelAttempt = nil
		peer.connected = err == nil && !peer.invalidated
		peer.lastError = err
		peer.notifyLocked()
		connected = peer.connected
		pending := peer.desiredRevision != peer.appliedRevision
		peer.mu.Unlock()
		if first {
			close(peer.first)
			first = false
		}
		if connected {
			backoff = 250 * time.Millisecond
			lastLog, suppressed = time.Time{}, 0
			if pending {
				continue
			}
			select {
			case <-ctx.Done():
			case <-peer.wake:
			}
			continue
		}
		peer.control.close()
		peer.observe(sourceControlResponse{}, err)
		now := time.Now()
		if lastLog.IsZero() || now.Sub(lastLog) >= 10*time.Second {
			if output := peer.source.server.settings.Stderr; output != nil {
				fmt.Fprintf(output, "camera source %s unavailable: %v (suppressed attempts: %d)\n", peer.source.config.ID, err, suppressed)
			}
			lastLog, suppressed = now, 0
		} else {
			suppressed++
		}
		delay := time.Duration(peer.rng.Int63n(int64(backoff)))
		backoff = min(30*time.Second, 2*backoff)
		timer := time.NewTimer(delay)
	wait:
		for {
			select {
			case <-ctx.Done():
				break wait
			case <-timer.C:
				break wait
			case <-peer.wake:
				// Viewer traffic cannot bypass backoff. Only an actual socket
				// change warrants an immediate rediscovery.
				peer.mu.Lock()
				event := peer.invalidated
				peer.mu.Unlock()
				if event {
					break wait
				}
			}
		}
		timer.Stop()
	}
}

func (source *mediaMTXSource) sourceConfig() SourceConfig {
	source.mu.Lock()
	defer source.mu.Unlock()
	return source.config
}
