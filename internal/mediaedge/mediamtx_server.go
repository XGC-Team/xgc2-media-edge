package mediaedge

import (
	"context"
	"errors"
	"fmt"
	"github.com/XGC-Team/xgc2-xrpc/go/httpx"
	"io"
	"math"
	"net"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	mtx "github.com/lxk36/xgc2-media-edge/internal/mediamtx"
)

const (
	defaultMediaMTXExecutable    = "/usr/lib/xgc2-media-edge/mediamtx"
	defaultMediaMTXRuntimeDir    = "/run/xgc2/media-edge/mediamtx"
	defaultMediaMTXAPIAddress    = "127.0.0.1:19997"
	defaultMediaMTXWHEPAddress   = "127.0.0.1:18889"
	defaultMediaMTXICEUDPAddress = "0.0.0.0:18189"
	mediaMTXReconcileInterval    = 250 * time.Millisecond
	mediaMTXSessionAppearTimeout = 3 * time.Second
	mediaMTXPathRequestTimeout   = 750 * time.Millisecond
	mediaMTXSessionCloseTimeout  = 2 * time.Second
	mediaMTXReconcileCloseLimit  = 4
)

// MediaMTXSettings are deployment boundaries, not camera-specific settings.
// Source IDs, topics, devices, encoders, and hardware remain in source adapters.
type MediaMTXSettings struct {
	Executable        string
	RuntimeDir        string
	APIAddress        string
	WHEPAddress       string
	ICEUDPAddress     string
	ICETCPAddress     string
	IPsFromInterfaces bool
	Stdout            io.Writer
	Stderr            io.Writer
}

type mediaMTXControl interface {
	Paths(context.Context) ([]mtx.PathStatus, error)
	WebRTCSessions(context.Context) ([]mtx.WebRTCSession, error)
	SetRecording(context.Context, string, bool) error
	ConfigureRecording(context.Context, string, mtx.RecordingSettings) error
	OpenWHEP(context.Context, string, string, string) (mtx.WHEPSession, error)
	CloseWHEP(context.Context, *url.URL) (bool, error)
	KickWebRTCSession(context.Context, string) error
}

type mediaMTXProcess interface {
	Start(context.Context) error
	Close() error
	Done() <-chan struct{}
	Err() error
}

// MediaMTXServer retains XGC Experiment/source lifecycle, metadata, snapshots,
// and recording intent while delegating RTP parsing, WebRTC/WHEP, fanout, ICE,
// and media container writing to upstream MediaMTX.
type MediaMTXServer struct {
	config   Config
	settings MediaMTXSettings
	control  mediaMTXControl
	process  mediaMTXProcess

	mu                sync.RWMutex
	sources           map[string]*mediaMTXSource
	sessions          map[string]*mediaMTXSession
	recordings        map[string]*mediaMTXRecording
	recordingHistory  map[string]RecordingManifest
	closing           bool
	operations        sync.WaitGroup
	background        sync.WaitGroup
	activeOperations  int
	pendingSessions   int
	cleanupCursor     int
	closeMu           sync.Mutex
	closeComplete     bool
	started           bool
	operationsDrained chan struct{}

	lifecycleContext context.Context
	cancelLifecycle  context.CancelFunc
	listener         net.Listener
	rpcHost          *httpx.Host
	instanceID       string
	httpServer       *httpServer
	stopOnce         sync.Once
	closed           chan struct{}
}

type mediaMTXSource struct {
	controlOnce sync.Once
	controlRPC  *cameraControl
	server      *MediaMTXServer
	config      SourceConfig

	lifecycleMu          sync.Mutex
	recordingLifecycleMu sync.Mutex
	mu                   sync.Mutex
	sessions             map[string]struct{}
	pending              int
	active               bool
	deactivateUncertain  bool
	shutdownStopped      bool
	recordingID          string

	deactivateTimer *time.Timer
	deactivateEpoch uint64
	activeSince     time.Time
	lastPacketAt    time.Time
	inboundBytes    uint64
	framesInError   uint64
	available       bool
	online          bool

	lastKeyframeRequestAt time.Time
	lastRecoveryAttemptAt time.Time
	recoveryPending       bool
	keyframePending       bool
	capturePending        bool
	statusPending         bool
	controlHealthy        bool
	controlError          string
	controlObservedAt     time.Time
	nativeStatus          sourceControlResponse
	snapshots             map[string]Snapshot
	snapshotOrder         []string
}

type mediaMTXSession struct {
	id         string
	upstreamID string
	location   *url.URL
	source     *mediaMTXSource
	createdAt  time.Time

	mu                       sync.Mutex
	keyframeAfterICE         bool
	closeRequested           bool
	closed                   bool
	opening                  bool
	observedID               string
	creationResponseComplete bool
}

func NewMediaMTX(config Config, settings MediaMTXSettings) (*MediaMTXServer, error) {
	normalized, err := config.normalized()
	if err != nil {
		return nil, err
	}
	settings = normalizedMediaMTXSettings(settings)

	paths := make([]mtx.Path, 0, len(normalized.Sources))
	for _, source := range normalized.Sources {
		path := mtx.Path{Name: source.ID, RTPAddress: source.RTPListenAddress}
		if normalized.Recording.enabled() {
			path.RecordPath = filepathJoinSlash(
				normalized.Recording.Root, "mediamtx", "%path", "%Y-%m-%d_%H-%M-%S-%f",
			)
		}
		paths = append(paths, path)
	}
	iceServers := make([]mtx.ICEServer, 0)
	for _, server := range normalized.ICEServers {
		for _, iceURL := range server.URLs {
			iceServers = append(iceServers, mtx.ICEServer{
				URL: iceURL, Username: server.Username, Password: fmt.Sprint(server.Credential),
			})
		}
	}
	rendered, err := mtx.Render(mtx.Config{
		APIAddress: settings.APIAddress, WHEPAddress: settings.WHEPAddress,
		ICEUDPAddress: settings.ICEUDPAddress, ICETCPAddress: settings.ICETCPAddress,
		AllowedOrigins: normalized.AllowedOrigins, AdditionalHosts: normalized.PublicIPs,
		ICEServers: iceServers, IPsFromInterfaces: settings.IPsFromInterfaces, Paths: paths,
	})
	if err != nil {
		return nil, err
	}
	control, err := mtx.NewClient(httpURLForAddress(settings.APIAddress), httpURLForAddress(settings.WHEPAddress))
	if err != nil {
		return nil, err
	}
	readiness := func(ctx context.Context) error {
		statuses, err := control.Paths(ctx)
		if err != nil {
			return err
		}
		found := make(map[string]struct{}, len(statuses))
		for _, status := range statuses {
			found[status.Name] = struct{}{}
		}
		for _, source := range normalized.Sources {
			if _, exists := found[source.ID]; !exists {
				return fmt.Errorf("configured MediaMTX path %q is missing", source.ID)
			}
		}
		return nil
	}
	process, err := mtx.NewProcess(mtx.ProcessConfig{
		Executable: settings.Executable, RuntimeDir: settings.RuntimeDir,
		Configuration: rendered, Readiness: readiness, Stdout: settings.Stdout, Stderr: settings.Stderr,
	})
	if err != nil {
		return nil, err
	}
	return newMediaMTXServer(normalized, settings, control, process), nil
}

func normalizedMediaMTXSettings(settings MediaMTXSettings) MediaMTXSettings {
	settings.Executable = strings.TrimSpace(settings.Executable)
	settings.RuntimeDir = strings.TrimSpace(settings.RuntimeDir)
	settings.APIAddress = strings.TrimSpace(settings.APIAddress)
	settings.WHEPAddress = strings.TrimSpace(settings.WHEPAddress)
	settings.ICEUDPAddress = strings.TrimSpace(settings.ICEUDPAddress)
	settings.ICETCPAddress = strings.TrimSpace(settings.ICETCPAddress)
	if settings.Executable == "" {
		settings.Executable = defaultMediaMTXExecutable
	}
	if settings.RuntimeDir == "" {
		settings.RuntimeDir = defaultMediaMTXRuntimeDir
	}
	if settings.APIAddress == "" {
		settings.APIAddress = defaultMediaMTXAPIAddress
	}
	if settings.WHEPAddress == "" {
		settings.WHEPAddress = defaultMediaMTXWHEPAddress
	}
	if settings.ICEUDPAddress == "" {
		settings.ICEUDPAddress = defaultMediaMTXICEUDPAddress
	}
	return settings
}

func newMediaMTXServer(
	config Config,
	settings MediaMTXSettings,
	control mediaMTXControl,
	process mediaMTXProcess,
) *MediaMTXServer {
	if config.MaxSessions == 0 {
		config.MaxSessions = defaultMaxSessions
	}
	if config.MaxOperations == 0 {
		config.MaxOperations = defaultMaxOperations
	}
	if config.RuntimePolicy != nil {
		if limit, err := config.RuntimePolicy.Integer("HOST_MAX_IN_FLIGHT"); err == nil && limit < int64(config.MaxOperations) {
			config.MaxOperations = int(limit)
		}
	}
	if config.MaxCaptureBytes == 0 {
		config.MaxCaptureBytes = 64 << 20
	}
	if config.MaxRetainedSnapshotBytes == 0 {
		config.MaxRetainedSnapshotBytes = 128 << 20
	}
	if config.RuntimePolicy != nil {
		if limit, err := config.RuntimePolicy.Integer("MAX_RESPONSE_BYTES"); err == nil && limit < config.MaxCaptureBytes {
			config.MaxCaptureBytes = limit
		}
	}
	lifecycleContext, cancelLifecycle := context.WithCancel(context.Background())
	server := &MediaMTXServer{
		config: config, settings: settings, control: control, process: process,
		sources:          make(map[string]*mediaMTXSource, len(config.Sources)),
		sessions:         make(map[string]*mediaMTXSession),
		recordings:       make(map[string]*mediaMTXRecording),
		recordingHistory: make(map[string]RecordingManifest),
		lifecycleContext: lifecycleContext, cancelLifecycle: cancelLifecycle,
		closed: make(chan struct{}),
	}
	for _, source := range config.Sources {
		server.sources[source.ID] = &mediaMTXSource{
			server: server, config: source, sessions: make(map[string]struct{}),
			snapshots: make(map[string]Snapshot),
		}
	}
	return server
}

// Start validates every adapter contract before MediaMTX binds ICE. This keeps
// a typo or mismatched source from creating a superficially healthy endpoint.
func (server *MediaMTXServer) Start() (startErr error) {
	if server == nil || server.control == nil || server.process == nil {
		return errors.New("MediaMTX server is not configured")
	}
	server.closeMu.Lock()
	defer server.closeMu.Unlock()
	if server.started || server.closing {
		return errors.New("media edge may be started once per process instance")
	}
	server.started = true
	childStarted := false
	defer func() {
		if startErr == nil {
			return
		}
		server.mu.Lock()
		server.closing = true
		server.cancelLifecycle()
		server.mu.Unlock()
		if server.httpServer != nil {
			_ = server.httpServer.close()
		}
		if server.rpcHost != nil {
			_ = server.closeRPC()
		}
		if server.listener != nil {
			_ = server.listener.Close()
		}
		for _, source := range server.sources {
			if source.controlRPC != nil {
				source.controlRPC.close()
			}
		}
		if childStarted {
			_ = server.process.Close()
		}
	}()
	if server.config.Recording.enabled() {
		if err := server.prepareMediaMTXRecording(); err != nil {
			return err
		}
	}
	sourceIDs := make([]string, 0, len(server.sources))
	for sourceID := range server.sources {
		sourceIDs = append(sourceIDs, sourceID)
	}
	sort.Strings(sourceIDs)
	for _, sourceID := range sourceIDs {
		source := server.sources[sourceID]
		described, err := describeSourceWith(server.lifecycleContext, source.config, source.cameraControl())
		if err != nil {
			return fmt.Errorf("validate media source %q: %w", source.config.ID, err)
		}
		source.config = described
		source.controlHealthy = true
		for index := range server.config.Sources {
			if server.config.Sources[index].ID == sourceID {
				server.config.Sources[index] = described
				break
			}
		}
	}
	listener, err := net.Listen("tcp", server.config.ControlAddress)
	if err != nil {
		return fmt.Errorf("listen for media edge control: %w", err)
	}
	server.listener = listener
	if err := server.process.Start(server.lifecycleContext); err != nil {
		_ = listener.Close()
		server.listener = nil
		return fmt.Errorf("start MediaMTX media kernel: %w", err)
	}
	childStarted = true
	if err := server.startRPC(); err != nil {
		_ = listener.Close()
		server.listener = nil
		_ = server.process.Close()
		return err
	}
	server.httpServer = newHTTPServer(server)
	if err := server.httpServer.start(listener); err != nil {
		return err
	}
	go func() {
		if err := server.httpServer.host.Wait(); err != nil && !errors.Is(err, net.ErrClosed) {
			_ = server.Close()
		}
	}()
	go server.reconcileLoop()
	go func() {
		select {
		case <-server.process.Done():
			_ = server.Close()
		case <-server.closed:
		}
	}()
	return nil
}

func (server *MediaMTXServer) HTTPConfig() Config {
	if server == nil {
		return Config{}
	}
	config := server.config
	config.AllowedOrigins = append([]string(nil), config.AllowedOrigins...)
	config.Sources = append([]SourceConfig(nil), config.Sources...)
	return config
}

func (server *MediaMTXServer) ControlAddress() string {
	if server == nil || server.listener == nil {
		return ""
	}
	return server.listener.Addr().String()
}

func (server *MediaMTXServer) RTPAddress(sourceID string) string {
	source := server.source(sourceID)
	if source == nil {
		return ""
	}
	return source.config.RTPListenAddress
}

func (server *MediaMTXServer) source(sourceID string) *mediaMTXSource {
	server.mu.RLock()
	defer server.mu.RUnlock()
	return server.sources[sourceID]
}

func (server *MediaMTXServer) beginOperation(parent context.Context) (context.Context, func(), error) {
	server.mu.Lock()
	if server.closing {
		server.mu.Unlock()
		return nil, nil, errors.New("media edge is closing")
	}
	if server.activeOperations >= server.config.MaxOperations {
		server.mu.Unlock()
		return nil, nil, ErrMediaCapacity
	}
	server.activeOperations++
	server.operations.Add(1)
	lifecycleContext := server.lifecycleContext
	server.mu.Unlock()
	operationContext, cancel := context.WithCancel(parent)
	stopLifecycleCancel := context.AfterFunc(lifecycleContext, cancel)
	return operationContext, func() {
		_ = stopLifecycleCancel()
		cancel()
		server.mu.Lock()
		server.activeOperations--
		server.mu.Unlock()
		server.operations.Done()
	}, nil
}

func (server *MediaMTXServer) isClosing() bool {
	server.mu.RLock()
	defer server.mu.RUnlock()
	return server.closing
}

func (server *MediaMTXServer) startBackground(work func()) bool {
	server.mu.Lock()
	if server.closing {
		server.mu.Unlock()
		return false
	}
	server.background.Add(1)
	server.mu.Unlock()
	go func() { defer server.background.Done(); work() }()
	return true
}

func (server *MediaMTXServer) OpenSession(
	ctx context.Context,
	sourceID string,
	offer SessionOffer,
) (SessionAnswer, error) {
	if strings.TrimSpace(offer.SDP) == "" || len(offer.SDP) > 256<<10 {
		return SessionAnswer{}, errors.New("WebRTC offer SDP is required and must be at most 256 KiB")
	}
	operationContext, finishOperation, err := server.beginOperation(ctx)
	if err != nil {
		return SessionAnswer{}, err
	}
	defer finishOperation()
	source := server.source(sourceID)
	if source == nil {
		return SessionAnswer{}, fmt.Errorf("media source %q was not found", sourceID)
	}
	server.mu.Lock()
	if len(server.sessions)+server.pendingSessions >= server.config.MaxSessions {
		server.mu.Unlock()
		return SessionAnswer{}, ErrMediaCapacity
	}
	server.pendingSessions++
	server.mu.Unlock()
	reserved := true
	defer func() {
		if reserved {
			server.mu.Lock()
			server.pendingSessions--
			server.mu.Unlock()
		}
	}()
	if err := source.acquire(operationContext); err != nil {
		return SessionAnswer{}, fmt.Errorf("activate media source %q: %w", sourceID, err)
	}
	pending := true
	defer func() {
		if pending {
			source.releasePending("")
		}
	}()
	source.requestKeyframeAsync(true)
	if err := server.waitForMediaMTXPath(operationContext, sourceID); err != nil {
		return SessionAnswer{}, fmt.Errorf("activate media source %q: %w", sourceID, err)
	}

	sessionID, err := newSnapshotID()
	if err != nil {
		return SessionAnswer{}, err
	}
	item := &mediaMTXSession{id: sessionID, source: source, createdAt: time.Now(), opening: true}
	server.mu.Lock()
	if server.closing {
		server.mu.Unlock()
		return SessionAnswer{}, errors.New("media edge is closing")
	}
	server.sessions[sessionID] = item
	server.pendingSessions--
	reserved = false
	server.mu.Unlock()
	source.releasePending(sessionID)
	pending = false
	// Own the token before native WHEP dispatch. A created session whose
	// response is lost still owns its bounded slot and source demand.
	upstream, err := server.control.OpenWHEP(operationContext, sourceID, offer.SDP, sessionID)
	item.mu.Lock()
	item.opening = false
	if err != nil {
		item.closeRequested = true
		var received *mtx.HTTPError
		item.creationResponseComplete = errors.As(err, &received)
		item.mu.Unlock()
		return SessionAnswer{}, fmt.Errorf("open MediaMTX WHEP session: %w", err)
	}
	item.location = upstream.Location
	item.upstreamID = pathBase(upstream.Location.Path)
	if server.isClosing() {
		item.closeRequested = true
	}
	closing := item.closeRequested
	item.mu.Unlock()
	if closing {
		_ = server.closeSession(item, true)
		return SessionAnswer{}, errors.New("media edge is closing")
	}
	// A short GOP already guarantees bounded startup. This request accelerates
	// first paint; reconcile sends one more after ICE is actually established.
	source.requestKeyframeAsync(true)
	return SessionAnswer{
		SessionID: sessionID, SDP: upstream.AnswerSDP, DataChannelLabel: ControlDataChannelLabel,
		Source: sourceDescription{ID: source.config.ID, Width: source.config.Width, Height: source.config.Height,
			FPS: source.config.FPS, FrameID: source.config.FrameID, Codec: sourceCodec},
	}, nil
}

func (server *MediaMTXServer) waitForMediaMTXPath(ctx context.Context, sourceID string) error {
	waitContext, cancel := context.WithTimeout(ctx, server.config.SessionGatherTimeout)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		paths, err := server.control.Paths(waitContext)
		if err == nil {
			for _, path := range paths {
				if path.Name != sourceID || !path.Available {
					continue
				}
				for _, track := range path.Tracks {
					if track.Codec == sourceCodec {
						return nil
					}
				}
				lastErr = errors.New("MediaMTX path is available without an H264 track")
			}
		} else {
			lastErr = err
		}
		select {
		case <-waitContext.Done():
			if lastErr != nil {
				return fmt.Errorf("wait for MediaMTX H264 path: %w: last probe: %v", waitContext.Err(), lastErr)
			}
			return fmt.Errorf("wait for MediaMTX H264 path: %w", waitContext.Err())
		case <-ticker.C:
		}
	}
}

func (server *MediaMTXServer) CloseSession(sessionID string) (bool, error) {
	server.mu.RLock()
	item := server.sessions[sessionID]
	server.mu.RUnlock()
	if item == nil {
		return false, nil
	}
	return true, server.closeSession(item, true)
}

func (server *MediaMTXServer) closeSession(item *mediaMTXSession, closeUpstream bool) error {
	return server.closeSessionContext(context.Background(), item, closeUpstream)
}

func (server *MediaMTXServer) closeSessionContext(parent context.Context, item *mediaMTXSession, closeUpstream bool) error {
	if item == nil {
		return nil
	}
	item.mu.Lock()
	defer item.mu.Unlock()
	if item.closed {
		return nil
	}
	item.closeRequested = true
	if item.opening && closeUpstream {
		return errors.New("media session negotiation is still in progress")
	}
	if closeUpstream {
		ctx, cancel := context.WithTimeout(parent, mediaMTXSessionCloseTimeout)
		var err error
		if item.location != nil {
			_, err = server.control.CloseWHEP(ctx, item.location)
		} else {
			err = server.closeUnknownSession(ctx, item)
		}
		cancel()
		if err != nil {
			// Keep both ownership records until deletion is confirmed. The
			// reconciliation loop retries even after the HTTP caller goes away.
			return fmt.Errorf("close media session: %w", err)
		}
	}
	item.closed = true
	item.source.removeSession(item.id)
	server.mu.Lock()
	delete(server.sessions, item.id)
	server.mu.Unlock()
	return nil
}

func (server *MediaMTXServer) closeUnknownSession(ctx context.Context, item *mediaMTXSession) error {
	sessions, err := server.control.WebRTCSessions(ctx)
	if err != nil {
		return err
	}
	var matched *mtx.WebRTCSession
	for _, actual := range sessions {
		query, err := url.ParseQuery(strings.TrimPrefix(actual.Query, "?"))
		if err != nil {
			continue
		}
		if len(query["xgcSession"]) != 1 || query.Get("xgcSession") != item.id {
			continue
		}
		if actual.Path != item.source.config.ID || actual.State != "read" {
			return errors.New("uncertain WHEP session identity mismatch")
		}
		if matched != nil || (item.observedID != "" && actual.ID != item.observedID) {
			return errors.New("uncertain WHEP session token is not unique")
		}
		candidate := actual
		matched = &candidate
	}
	if matched == nil {
		if item.observedID != "" || item.creationResponseComplete {
			// This token's native creation was previously observed. Complete
			// inventory absence confirms deletion even if the kick reply was lost.
			// A full native HTTP failure response also finishes creation before
			// the sequential native inventory owner can answer this probe.
			return nil
		}
		return errors.New("WHEP creation outcome is still unknown; ownership retained")
	}
	item.observedID = matched.ID
	return server.control.KickWebRTCSession(ctx, matched.ID)
}

func (source *mediaMTXSource) acquire(ctx context.Context) error {
	source.lifecycleMu.Lock()
	defer source.lifecycleMu.Unlock()
	if source.server.isClosing() {
		return errors.New("media edge is closing")
	}
	source.mu.Lock()
	source.pending++
	source.cancelDeactivateTimerLocked()
	if source.active && !source.deactivateUncertain {
		source.mu.Unlock()
		return nil
	}
	source.mu.Unlock()
	if _, _, _, err := source.cameraControl().call(ctx, sourceControlRequest{
		Operation: "start",
	}); err != nil {
		source.mu.Lock()
		source.pending--
		source.active = true // a lost reply can follow applied activation
		source.deactivateUncertain = true
		unused := !source.hasConsumersLocked()
		source.mu.Unlock()
		if unused {
			source.scheduleDeactivate()
		}
		return err
	}
	source.mu.Lock()
	source.active = true
	source.deactivateUncertain = false
	source.activeSince = time.Now()
	source.lastRecoveryAttemptAt = time.Time{}
	source.mu.Unlock()
	return nil
}

func (source *mediaMTXSource) releasePending(sessionID string) {
	source.mu.Lock()
	if source.pending > 0 {
		source.pending--
	}
	if sessionID != "" {
		source.sessions[sessionID] = struct{}{}
	}
	unused := !source.hasConsumersLocked() && source.active
	source.mu.Unlock()
	if unused {
		source.scheduleDeactivate()
	}
}

func (source *mediaMTXSource) removeSession(sessionID string) {
	source.mu.Lock()
	delete(source.sessions, sessionID)
	empty := !source.hasConsumersLocked()
	source.mu.Unlock()
	if empty {
		source.scheduleDeactivate()
	}
}

func (source *mediaMTXSource) hasConsumersLocked() bool {
	return source.pending != 0 || len(source.sessions) != 0 || source.recordingID != ""
}

func (source *mediaMTXSource) consumerCountLocked() int {
	count := len(source.sessions)
	if source.recordingID != "" {
		count++
	}
	return count
}

func (source *mediaMTXSource) cancelDeactivateTimerLocked() {
	source.deactivateEpoch++
	if source.deactivateTimer != nil {
		source.deactivateTimer.Stop()
		source.deactivateTimer = nil
	}
}

func (source *mediaMTXSource) scheduleDeactivate() {
	if source.server.isClosing() {
		return
	}
	source.mu.Lock()
	if !source.active || source.hasConsumersLocked() {
		source.mu.Unlock()
		return
	}
	source.cancelDeactivateTimerLocked()
	epoch := source.deactivateEpoch
	source.deactivateTimer = time.AfterFunc(source.server.config.SessionGracePeriod, func() {
		source.deactivateIfUnused(epoch)
	})
	source.mu.Unlock()
}

func (source *mediaMTXSource) deactivateIfUnused(epoch uint64) {
	source.lifecycleMu.Lock()
	defer source.lifecycleMu.Unlock()
	if source.server.isClosing() {
		return
	}
	source.mu.Lock()
	if epoch != source.deactivateEpoch || !source.active || source.hasConsumersLocked() {
		source.mu.Unlock()
		return
	}
	source.deactivateTimer = nil
	source.mu.Unlock()
	ctx, cancel := context.WithTimeout(source.server.lifecycleContext, sourceControlRequestTimeout)
	defer cancel()
	_, _, _, err := source.cameraControl().call(ctx, sourceControlRequest{
		Operation: "stop",
	})
	source.mu.Lock()
	defer source.mu.Unlock()
	if err != nil {
		// A lost reply does not prove whether the adapter stopped. Keep the
		// lease active, retry stop, and make a new acquire confirm activation.
		source.deactivateUncertain = true
		if source.server.lifecycleContext.Err() == nil && epoch == source.deactivateEpoch && !source.hasConsumersLocked() {
			source.deactivateTimer = time.AfterFunc(sourceRecoveryMinimumInterval, func() {
				source.deactivateIfUnused(epoch)
			})
		}
		return
	}
	source.active = false
	source.deactivateUncertain = false
	source.activeSince = time.Time{}
	source.lastRecoveryAttemptAt = time.Time{}
}

func (source *mediaMTXSource) requestKeyframeAsync(force bool) {
	if !source.config.KeyframeRequestSupported {
		return
	}
	now := time.Now()
	source.mu.Lock()
	if !source.active || source.keyframePending || (!force && !source.lastKeyframeRequestAt.IsZero() &&
		now.Sub(source.lastKeyframeRequestAt) < keyframeRequestMinimumInterval) {
		source.mu.Unlock()
		return
	}
	source.lastKeyframeRequestAt = now
	source.keyframePending = true
	source.mu.Unlock()
	if !source.server.startBackground(func() {
		defer func() { source.mu.Lock(); source.keyframePending = false; source.mu.Unlock() }()
		ctx, cancel := context.WithTimeout(source.server.lifecycleContext, sourceControlRequestTimeout)
		defer cancel()
		_, _, _, _ = source.cameraControl().call(ctx, sourceControlRequest{
			Operation: "request-keyframe",
		})
	}) {
		source.mu.Lock()
		source.keyframePending = false
		source.mu.Unlock()
	}
}

func (server *MediaMTXServer) reconcileLoop() {
	ticker := time.NewTicker(mediaMTXReconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			server.reconcile()
		case <-server.closed:
			return
		}
	}
}

func (server *MediaMTXServer) reconcile() {
	for _, source := range server.sources {
		source.pollStatusAsync()
	}
	ctx, cancel := context.WithTimeout(server.lifecycleContext, mediaMTXPathRequestTimeout)
	paths, pathErr := server.control.Paths(ctx)
	sessions, sessionErr := server.control.WebRTCSessions(ctx)
	cancel()
	now := time.Now()
	if pathErr == nil {
		observed := make(map[string]bool, len(paths))
		for _, status := range paths {
			if source := server.source(status.Name); source != nil {
				observed[status.Name] = true
				source.observePath(now, status)
			}
		}
		for sourceID, source := range server.sources {
			if !observed[sourceID] {
				source.mu.Lock()
				source.available = false
				source.online = false
				source.mu.Unlock()
			}
		}
	} else {
		for _, source := range server.sources {
			source.mu.Lock()
			source.available = false
			source.online = false
			source.mu.Unlock()
		}
	}
	upstream := make(map[string]mtx.WebRTCSession, len(sessions))
	upstreamByToken := make(map[string]mtx.WebRTCSession, len(sessions))
	for _, session := range sessions {
		upstream[session.ID] = session
		query, err := url.ParseQuery(strings.TrimPrefix(session.Query, "?"))
		if err == nil {
			if token := strings.TrimSpace(query.Get("xgcSession")); token != "" {
				upstreamByToken[token] = session
			}
		}
	}
	server.mu.RLock()
	local := make([]*mediaMTXSession, 0, len(server.sessions))
	for _, session := range server.sessions {
		local = append(local, session)
	}
	server.mu.RUnlock()
	sort.Slice(local, func(left, right int) bool { return local[left].id < local[right].id })
	server.mu.Lock()
	start := 0
	if len(local) != 0 {
		start = server.cleanupCursor % len(local)
	}
	server.mu.Unlock()
	cleanupContext, cleanupCancel := context.WithTimeout(server.lifecycleContext, mediaMTXSessionCloseTimeout)
	defer cleanupCancel()
	cleanupAttempts := 0
	for index := range local {
		session := local[(start+index)%len(local)]
		session.mu.Lock()
		closeRequested := session.closeRequested
		opening := session.opening
		session.mu.Unlock()
		if opening {
			continue
		}
		if closeRequested {
			if cleanupAttempts < mediaMTXReconcileCloseLimit && cleanupContext.Err() == nil {
				cleanupAttempts++
				server.mu.Lock()
				server.cleanupCursor = (start + index + 1) % len(local)
				server.mu.Unlock()
				_ = server.closeSessionContext(cleanupContext, session, true)
			}
			continue
		}
		if sessionErr != nil {
			continue
		}
		actual, found := upstreamByToken[session.id]
		if !found {
			actual, found = upstream[session.upstreamID]
		}
		if !found {
			if now.Sub(session.createdAt) >= mediaMTXSessionAppearTimeout {
				server.closeSession(session, false)
			}
			continue
		}
		if actual.Path != session.source.config.ID || actual.State != "read" {
			session.mu.Lock()
			session.closeRequested = true
			session.mu.Unlock()
			continue
		}
		if actual.PeerConnectionEstablished {
			session.mu.Lock()
			request := !session.keyframeAfterICE
			session.keyframeAfterICE = true
			session.mu.Unlock()
			if request {
				session.source.requestKeyframeAsync(true)
			}
		}
	}
}

func (source *mediaMTXSource) observePath(now time.Time, status mtx.PathStatus) {
	source.mu.Lock()
	bytesChanged := status.InboundBytes != source.inboundBytes
	if bytesChanged {
		source.lastPacketAt = now
	}
	source.inboundBytes = status.InboundBytes
	source.framesInError = status.InboundFramesInError
	source.available = status.Available
	source.online = status.Online
	recordingID := source.recordingID
	lastActivity := source.lastPacketAt
	if lastActivity.Before(source.activeSince) {
		lastActivity = source.activeSince
	}
	recover := !source.recoveryPending && source.active && source.consumerCountLocked() > 0 && !lastActivity.IsZero() &&
		now.Sub(lastActivity) >= sourceStallTimeout &&
		(source.lastRecoveryAttemptAt.IsZero() || now.Sub(source.lastRecoveryAttemptAt) >= sourceRecoveryMinimumInterval)
	recoveryEpoch := source.deactivateEpoch
	if recover {
		source.lastRecoveryAttemptAt = now
		source.recoveryPending = true
	}
	source.mu.Unlock()
	if bytesChanged && recordingID != "" {
		source.server.markMediaMTXRecordingActive(recordingID, now)
	}
	if recover {
		if !source.server.startBackground(func() { source.recover(recoveryEpoch) }) {
			source.mu.Lock()
			source.recoveryPending = false
			source.mu.Unlock()
		}
	}
}

func (source *mediaMTXSource) recover(epoch uint64) {
	source.lifecycleMu.Lock()
	defer source.lifecycleMu.Unlock()
	defer func() {
		source.mu.Lock()
		source.recoveryPending = false
		source.mu.Unlock()
	}()
	if source.server.isClosing() {
		return
	}
	// Demand may have disappeared or been replaced while this task waited
	// for lifecycleMu. Never let an old recovery resurrect an idle source.
	source.mu.Lock()
	current := epoch == source.deactivateEpoch && source.active && source.hasConsumersLocked()
	source.mu.Unlock()
	if !current {
		return
	}
	ctx, cancel := context.WithTimeout(source.server.lifecycleContext, sourceControlRequestTimeout)
	defer cancel()
	if _, _, _, err := source.cameraControl().call(ctx, sourceControlRequest{
		Operation: "start",
	}); err == nil {
		source.requestKeyframeAsync(true)
	}
}

func (server *MediaMTXServer) SourceStatuses() []SourceStatus {
	server.mu.RLock()
	sources := make([]*mediaMTXSource, 0, len(server.sources))
	for _, source := range server.sources {
		sources = append(sources, source)
	}
	server.mu.RUnlock()
	statuses := make([]SourceStatus, 0, len(sources))
	for _, source := range sources {
		source.mu.Lock()
		statuses = append(statuses, SourceStatus{
			ID: source.config.ID, Active: source.active, Available: source.available, Online: source.online,
			Consumers: source.consumerCountLocked(), Viewers: len(source.sessions), RecordingID: source.recordingID,
			LastPacketAt: source.lastPacketAt.UTC(), BytesReceived: source.inboundBytes,
			FramesInError: source.framesInError, Width: source.config.Width, Height: source.config.Height,
			FPS: source.config.FPS, FrameID: source.config.FrameID,
			StateUncertain: source.deactivateUncertain, ControlHealthy: source.controlHealthy,
			ControlError: source.controlError, ControlObservedAt: source.controlObservedAt,
			ControlInstanceID: source.cameraControl().boundInstance(), NativeState: source.nativeStatus.State,
			NativeAppliedActive: source.nativeStatus.AppliedActive, ConfigurationRevision: source.nativeStatus.ConfigurationRevision,
		})
		source.mu.Unlock()
	}
	sort.Slice(statuses, func(left, right int) bool { return statuses[left].ID < statuses[right].ID })
	return statuses
}

func (source *mediaMTXSource) pollStatusAsync() {
	if source.server.isClosing() {
		return
	}
	source.mu.Lock()
	if source.statusPending {
		source.mu.Unlock()
		return
	}
	source.statusPending = true
	source.mu.Unlock()
	if !source.server.startBackground(func() {
		response, _, _, err := source.cameraControl().call(source.server.lifecycleContext, sourceControlRequest{Operation: "status"})
		source.mu.Lock()
		defer source.mu.Unlock()
		source.statusPending = false
		source.controlObservedAt = time.Now().UTC()
		if err != nil {
			source.controlHealthy = false
			source.controlError = err.Error()
			if len(source.controlError) > 512 {
				source.controlError = source.controlError[:512]
			}
			return
		}
		if response.ManagedSourceID != source.config.ID {
			source.controlHealthy = false
			source.controlError = "source status identity mismatch"
			return
		}
		source.controlHealthy = true
		source.controlError = ""
		source.nativeStatus = response
	}) {
		source.mu.Lock()
		source.statusPending = false
		source.mu.Unlock()
	}
}

func (server *MediaMTXServer) CaptureSnapshot(
	ctx context.Context,
	sourceID string,
	request SnapshotCaptureRequest,
) (Snapshot, error) {
	operationContext, finishOperation, err := server.beginOperation(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	defer finishOperation()
	source := server.source(sourceID)
	if source == nil {
		return Snapshot{}, fmt.Errorf("media source %q was not found", sourceID)
	}
	source.mu.Lock()
	if source.capturePending {
		source.mu.Unlock()
		return Snapshot{}, ErrMediaCapacity
	}
	source.capturePending = true
	source.mu.Unlock()
	defer func() { source.mu.Lock(); source.capturePending = false; source.mu.Unlock() }()
	if err := source.acquire(operationContext); err != nil {
		return Snapshot{}, fmt.Errorf("activate media source %q: %w", sourceID, err)
	}
	defer source.releasePending("")
	id, err := newSnapshotID()
	if err != nil {
		return Snapshot{}, err
	}
	response, jpeg, rgb, err := source.cameraControl().call(operationContext, sourceControlRequest{
		Operation: "capture", SnapshotID: id, IncludeRGB: request.IncludeRGB,
		RequestKeyframe: request.RequestKeyframe, RequireFresh: request.RequireFresh,
	})
	if err != nil {
		return Snapshot{}, fmt.Errorf("capture source snapshot: %w", err)
	}
	if response.SourceID != source.config.ID || response.SnapshotID != id || response.Sequence == 0 {
		return Snapshot{}, errors.New("capture source returned a mismatched snapshot ID")
	}
	if response.Width != source.config.Width || response.Height != source.config.Height ||
		response.PixelFormat != "rgb8" || response.FrameID != source.config.FrameID {
		return Snapshot{}, errors.New("capture source snapshot metadata does not match the media source")
	}
	if request.includeRGB() {
		if response.RGBBytes != response.Width*response.Height*3 || len(rgb) != response.RGBBytes {
			return Snapshot{}, errors.New("capture source snapshot RGB does not match the media source")
		}
	} else if response.RGBBytes != 0 || len(rgb) != 0 {
		return Snapshot{}, errors.New("JPEG-only capture source returned forbidden RGB")
	}
	calibrationState := response.CalibrationState
	if calibrationState == "" {
		if len(response.CameraMatrix) == 0 && len(response.Distortion) == 0 {
			calibrationState = "unavailable"
		} else {
			calibrationState = "available"
		}
	}
	if calibrationState != "available" && calibrationState != "unavailable" {
		return Snapshot{}, errors.New("capture source calibration state is invalid")
	}
	if calibrationState == "available" && (len(response.CameraMatrix) != 9 || len(response.Distortion) < 4 || len(response.Distortion) > 16) {
		return Snapshot{}, errors.New("capture source camera intrinsics are invalid")
	}
	if calibrationState == "unavailable" && (len(response.CameraMatrix) != 0 || len(response.Distortion) != 0) {
		return Snapshot{}, errors.New("unavailable source calibration must omit intrinsics")
	}
	for _, value := range append(append([]float64(nil), response.CameraMatrix...), response.Distortion...) {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return Snapshot{}, errors.New("capture source camera intrinsics are non-finite")
		}
	}
	jpegBackend := strings.TrimSpace(response.JPEGBackend)
	jpegReadback := strings.TrimSpace(response.JPEGReadback)
	jpegFallbackReason := strings.TrimSpace(response.JPEGFallbackReason)
	if len(jpegBackend) > 64 || len(jpegReadback) > 64 ||
		len(jpegFallbackReason) > 512 || response.JPEGReadbackMillis < 0 ||
		response.JPEGEncodeMillis < 0 || math.IsNaN(response.JPEGReadbackMillis) ||
		math.IsNaN(response.JPEGEncodeMillis) || math.IsInf(response.JPEGReadbackMillis, 0) ||
		math.IsInf(response.JPEGEncodeMillis, 0) {
		return Snapshot{}, errors.New("capture source snapshot JPEG diagnostics are invalid")
	}
	snapshot := Snapshot{
		ID: id, SourceID: source.config.ID, FrameID: response.FrameID,
		TimestampNanoseconds: response.TimestampNanoseconds,
		TimestampClockDomain: strings.ToLower(strings.TrimSpace(response.TimestampClockDomain)),
		Width:                response.Width, Height: response.Height, PixelFormat: response.PixelFormat,
		JPEG: jpeg, RGB: rgb, CameraMatrix: append([]float64(nil), response.CameraMatrix...),
		JPEGBackend: jpegBackend, JPEGReadback: jpegReadback,
		JPEGFallbackReason: jpegFallbackReason,
		JPEGReadbackMillis: response.JPEGReadbackMillis,
		JPEGEncodeMillis:   response.JPEGEncodeMillis,
		Distortion:         append([]float64(nil), response.Distortion...),
		RenderPose:         cloneSnapshotRenderPose(response.RenderPose), PoseFrameID: response.PoseFrameID,
		CalibrationState: calibrationState, Sequence: response.Sequence,
		ExpiresAt: time.Now().Add(server.config.SnapshotTTL),
	}
	switch snapshot.TimestampClockDomain {
	case "simulation", "system_realtime", "monotonic", "device", "unknown":
	case "":
		snapshot.TimestampClockDomain = "unknown"
	default:
		return Snapshot{}, fmt.Errorf("capture source snapshot returned invalid timestamp clock domain %q", response.TimestampClockDomain)
	}
	if snapshot.TimestampNanoseconds < 0 {
		return Snapshot{}, errors.New("capture source snapshot returned a negative source timestamp")
	}
	if err := source.storeSnapshot(snapshot); err != nil {
		return Snapshot{}, err
	}
	return snapshot, nil
}

func (source *mediaMTXSource) storeSnapshot(snapshot Snapshot) error {
	server := source.server
	server.mu.Lock()
	defer server.mu.Unlock()
	bytes := int64(len(snapshot.JPEG)) + int64(len(snapshot.RGB))
	if bytes > server.config.MaxRetainedSnapshotBytes {
		return ErrMediaCapacity
	}
	// Retention has a product-wide byte budget and a two-item cap per source.
	// All snapshot mutations follow server -> source lock order.
	for _, item := range server.sources {
		item.mu.Lock()
	}
	defer func() {
		for _, item := range server.sources {
			item.mu.Unlock()
		}
	}()
	now := time.Now()
	var retained int64
	for _, item := range server.sources {
		order := item.snapshotOrder[:0]
		for _, id := range item.snapshotOrder {
			current, found := item.snapshots[id]
			if !found {
				continue
			}
			if !current.ExpiresAt.After(now) {
				delete(item.snapshots, id)
				continue
			}
			order = append(order, id)
			retained += int64(len(current.JPEG)) + int64(len(current.RGB))
		}
		item.snapshotOrder = order
	}
	source.snapshots[snapshot.ID] = snapshot
	source.snapshotOrder = append(source.snapshotOrder, snapshot.ID)
	retained += bytes
	remove := func(item *mediaMTXSource, id string) {
		current := item.snapshots[id]
		retained -= int64(len(current.JPEG)) + int64(len(current.RGB))
		delete(item.snapshots, id)
		for index, key := range item.snapshotOrder {
			if key == id {
				item.snapshotOrder = append(item.snapshotOrder[:index], item.snapshotOrder[index+1:]...)
				break
			}
		}
	}
	for len(source.snapshotOrder) > maximumSnapshots {
		remove(source, source.snapshotOrder[0])
	}
	for retained > server.config.MaxRetainedSnapshotBytes {
		var oldestSource *mediaMTXSource
		var oldest Snapshot
		for _, item := range server.sources {
			for _, current := range item.snapshots {
				if current.ID != snapshot.ID && (oldestSource == nil || current.ExpiresAt.Before(oldest.ExpiresAt)) {
					oldestSource = item
					oldest = current
				}
			}
		}
		if oldestSource == nil {
			return ErrMediaCapacity
		}
		remove(oldestSource, oldest.ID)
	}
	return nil
}

func (server *MediaMTXServer) Snapshot(snapshotID string) (Snapshot, bool) {
	server.mu.RLock()
	sources := make([]*mediaMTXSource, 0, len(server.sources))
	for _, source := range server.sources {
		sources = append(sources, source)
	}
	server.mu.RUnlock()
	for _, source := range sources {
		source.mu.Lock()
		snapshot, found := source.snapshots[snapshotID]
		if found && !snapshot.ExpiresAt.After(time.Now()) {
			delete(source.snapshots, snapshotID)
			found = false
		}
		source.mu.Unlock()
		if found {
			return snapshot, true
		}
	}
	return Snapshot{}, false
}

func (server *MediaMTXServer) DeleteSnapshot(snapshotID string) bool {
	server.mu.RLock()
	sources := make([]*mediaMTXSource, 0, len(server.sources))
	for _, source := range server.sources {
		sources = append(sources, source)
	}
	server.mu.RUnlock()
	for _, source := range sources {
		source.mu.Lock()
		if _, found := source.snapshots[snapshotID]; !found {
			source.mu.Unlock()
			continue
		}
		delete(source.snapshots, snapshotID)
		for index, id := range source.snapshotOrder {
			if id == snapshotID {
				source.snapshotOrder = append(source.snapshotOrder[:index], source.snapshotOrder[index+1:]...)
				break
			}
		}
		source.mu.Unlock()
		return true
	}
	return false
}

// Recording methods are implemented in mediamtx_recording.go so the HTTP
// contract remains identical while the media files are written by MediaMTX.

func (server *MediaMTXServer) Close() error {
	if server == nil {
		return nil
	}
	server.closeMu.Lock()
	defer server.closeMu.Unlock()
	if server.closeComplete {
		return nil
	}
	server.stopOnce.Do(func() {
		server.mu.Lock()
		server.closing = true
		server.cancelLifecycle()
		server.mu.Unlock()
		close(server.closed)
		server.operationsDrained = make(chan struct{})
		go func() { server.operations.Wait(); server.background.Wait(); close(server.operationsDrained) }()
	})
	var failures []error
	if server.httpServer != nil {
		if err := server.httpServer.close(); err != nil {
			failures = append(failures, err)
		}
	}
	if server.rpcHost != nil {
		if err := server.closeRPC(); err != nil {
			failures = append(failures, err)
		}
	}
	if server.listener != nil {
		_ = server.listener.Close()
	}
	select {
	case <-server.operationsDrained:
	case <-time.After(server.shutdownTimeout()):
		return errors.Join(append(failures, errors.New("media operations have not drained"))...)
	}
	server.mu.RLock()
	sessions := make([]*mediaMTXSession, 0, len(server.sessions))
	for _, session := range server.sessions {
		sessions = append(sessions, session)
	}
	sources := make([]*mediaMTXSource, 0, len(server.sources))
	for _, source := range server.sources {
		sources = append(sources, source)
	}
	server.mu.RUnlock()
	var sessionFailures []error
	cleanupContext, cleanupCancel := context.WithTimeout(context.Background(), server.shutdownTimeout())
	for _, session := range sessions {
		if cleanupContext.Err() != nil {
			sessionFailures = append(sessionFailures, cleanupContext.Err())
			break
		}
		if err := server.closeSessionContext(cleanupContext, session, true); err != nil {
			sessionFailures = append(sessionFailures, err)
		}
	}
	cleanupCancel()
	if err := server.stopAllMediaMTXRecordings(); err != nil {
		failures = append(failures, err)
	}
	for _, source := range sources {
		source.lifecycleMu.Lock()
		source.mu.Lock()
		source.cancelDeactivateTimerLocked()
		stopped := source.shutdownStopped
		if !source.active && !source.deactivateUncertain && !source.hasConsumersLocked() {
			stopped = true
			source.shutdownStopped = true
		}
		source.mu.Unlock()
		if !stopped {
			ctx, cancel := context.WithTimeout(context.Background(), sourceControlRequestTimeout)
			_, _, _, err := source.cameraControl().call(ctx, sourceControlRequest{Operation: "stop"})
			cancel()
			source.mu.Lock()
			if err != nil {
				source.deactivateUncertain = true
				failures = append(failures, fmt.Errorf("deactivate media source %q: %w", source.config.ID, err))
			} else {
				source.shutdownStopped = true
				source.active = false
				source.deactivateUncertain = false
				source.activeSince = time.Time{}
				source.lastRecoveryAttemptAt = time.Time{}
			}
			source.mu.Unlock()
			if err == nil {
				source.cameraControl().close()
			}
		}
		if stopped {
			source.cameraControl().close()
		}
		source.lifecycleMu.Unlock()
	}
	if err := server.process.Close(); err != nil {
		failures = append(failures, err)
		failures = append(failures, sessionFailures...)
	} else {
		// Confirmed child exit ends every WHEP connection even if an individual
		// DELETE failed. A source stop failure remains independently retryable.
		for _, session := range sessions {
			_ = server.closeSession(session, false)
		}
	}
	if closer, ok := server.control.(io.Closer); ok && len(failures) == 0 {
		if err := closer.Close(); err != nil {
			failures = append(failures, err)
		}
	}
	closeErr := errors.Join(failures...)
	server.closeComplete = closeErr == nil
	return closeErr
}

func httpURLForAddress(address string) string {
	return (&url.URL{Scheme: "http", Host: address}).String()
}

func pathBase(value string) string {
	value = strings.TrimRight(value, "/")
	if index := strings.LastIndexByte(value, '/'); index >= 0 {
		return value[index+1:]
	}
	return value
}

func filepathJoinSlash(elements ...string) string {
	return filepath.ToSlash(filepath.Join(elements...))
}

var _ httpBackend = (*MediaMTXServer)(nil)

func (source *mediaMTXSource) cameraControl() *cameraControl {
	source.controlOnce.Do(func() {
		source.controlRPC = &cameraControl{socket: source.config.ControlSocket, instance: source.config.ControlInstanceID, sourceID: source.config.ID, maxCaptureBytes: source.server.config.MaxCaptureBytes, policy: source.server.config.RuntimePolicy}
	})
	return source.controlRPC
}
