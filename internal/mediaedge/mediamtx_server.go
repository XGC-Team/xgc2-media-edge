package mediaedge

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	defaultMediaMTXRuntimeDir    = "/run/xgc2/media-edge"
	defaultMediaMTXAPIAddress    = "127.0.0.1:19997"
	defaultMediaMTXWHEPAddress   = "127.0.0.1:18889"
	defaultMediaMTXICEUDPAddress = "0.0.0.0:18189"
	mediaMTXReconcileInterval    = 250 * time.Millisecond
	mediaMTXSessionAppearTimeout = 3 * time.Second
	mediaMTXPathRequestTimeout   = 750 * time.Millisecond
	mediaMTXSessionCloseTimeout  = 2 * time.Second
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

	mu               sync.RWMutex
	sources          map[string]*mediaMTXSource
	sessions         map[string]*mediaMTXSession
	recordings       map[string]*mediaMTXRecording
	recordingHistory map[string]RecordingManifest
	closing          bool
	operations       sync.WaitGroup

	lifecycleContext context.Context
	cancelLifecycle  context.CancelFunc
	listener         net.Listener
	httpServer       *httpServer
	closeOnce        sync.Once
	closed           chan struct{}
}

type mediaMTXSource struct {
	server *MediaMTXServer
	config SourceConfig

	lifecycleMu          sync.Mutex
	recordingLifecycleMu sync.Mutex
	mu                   sync.Mutex
	sessions             map[string]struct{}
	pending              int
	active               bool
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
	snapshots             map[string]Snapshot
	snapshotOrder         []string
}

type mediaMTXSession struct {
	id         string
	upstreamID string
	location   *url.URL
	source     *mediaMTXSource
	createdAt  time.Time

	mu               sync.Mutex
	keyframeAfterICE bool
	closeOnce        sync.Once
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
func (server *MediaMTXServer) Start() error {
	if server == nil || server.control == nil || server.process == nil {
		return errors.New("MediaMTX server is not configured")
	}
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
		described, err := describeSource(server.lifecycleContext, source.config)
		if err != nil {
			return fmt.Errorf("validate media source %q: %w", source.config.ID, err)
		}
		source.config = described
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
	server.httpServer = newHTTPServer(server)
	go func() {
		if err := server.httpServer.serve(listener); err != nil && !errors.Is(err, net.ErrClosed) {
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
	server.operations.Add(1)
	lifecycleContext := server.lifecycleContext
	server.mu.Unlock()
	operationContext, cancel := context.WithCancel(parent)
	stopLifecycleCancel := context.AfterFunc(lifecycleContext, cancel)
	return operationContext, func() {
		_ = stopLifecycleCancel()
		cancel()
		server.operations.Done()
	}, nil
}

func (server *MediaMTXServer) isClosing() bool {
	server.mu.RLock()
	defer server.mu.RUnlock()
	return server.closing
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
	upstream, err := server.control.OpenWHEP(operationContext, sourceID, offer.SDP, sessionID)
	if err != nil {
		return SessionAnswer{}, fmt.Errorf("open MediaMTX WHEP session: %w", err)
	}
	item := &mediaMTXSession{
		id: sessionID, upstreamID: pathBase(upstream.Location.Path), location: upstream.Location,
		source: source, createdAt: time.Now(),
	}
	server.mu.Lock()
	if server.closing {
		server.mu.Unlock()
		closeContext, cancel := context.WithTimeout(context.Background(), mediaMTXSessionCloseTimeout)
		_, _ = server.control.CloseWHEP(closeContext, upstream.Location)
		cancel()
		return SessionAnswer{}, errors.New("media edge is closing")
	}
	server.sessions[sessionID] = item
	server.mu.Unlock()
	source.releasePending(sessionID)
	pending = false
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

func (server *MediaMTXServer) CloseSession(sessionID string) bool {
	server.mu.RLock()
	item := server.sessions[sessionID]
	server.mu.RUnlock()
	if item == nil {
		return false
	}
	server.closeSession(item, true)
	return true
}

func (server *MediaMTXServer) closeSession(item *mediaMTXSession, closeUpstream bool) {
	if item == nil {
		return
	}
	item.closeOnce.Do(func() {
		if closeUpstream {
			ctx, cancel := context.WithTimeout(context.Background(), mediaMTXSessionCloseTimeout)
			_, _ = server.control.CloseWHEP(ctx, item.location)
			cancel()
		}
		item.source.removeSession(item.id)
		server.mu.Lock()
		delete(server.sessions, item.id)
		server.mu.Unlock()
	})
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
	if source.active {
		source.mu.Unlock()
		return nil
	}
	source.mu.Unlock()
	active := true
	if _, _, _, err := callSourceControl(ctx, source.config.ControlSocket, sourceControlRequest{
		Operation: "set-active", Active: &active,
	}); err != nil {
		source.mu.Lock()
		source.pending--
		source.mu.Unlock()
		return err
	}
	source.mu.Lock()
	source.active = true
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
	source.active = false
	source.activeSince = time.Time{}
	source.lastRecoveryAttemptAt = time.Time{}
	source.mu.Unlock()
	ctx, cancel := context.WithTimeout(source.server.lifecycleContext, sourceControlRequestTimeout)
	defer cancel()
	active := false
	_, _, _, _ = callSourceControl(ctx, source.config.ControlSocket, sourceControlRequest{
		Operation: "set-active", Active: &active,
	})
}

func (source *mediaMTXSource) requestKeyframeAsync(force bool) {
	now := time.Now()
	source.mu.Lock()
	if !source.active || (!force && !source.lastKeyframeRequestAt.IsZero() &&
		now.Sub(source.lastKeyframeRequestAt) < keyframeRequestMinimumInterval) {
		source.mu.Unlock()
		return
	}
	source.lastKeyframeRequestAt = now
	source.mu.Unlock()
	go func() {
		ctx, cancel := context.WithTimeout(source.server.lifecycleContext, sourceControlRequestTimeout)
		defer cancel()
		_, _, _, _ = callSourceControl(ctx, source.config.ControlSocket, sourceControlRequest{
			Operation: "request-keyframe",
		})
	}()
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
	ctx, cancel := context.WithTimeout(server.lifecycleContext, mediaMTXPathRequestTimeout)
	paths, pathErr := server.control.Paths(ctx)
	sessions, sessionErr := server.control.WebRTCSessions(ctx)
	cancel()
	now := time.Now()
	if pathErr == nil {
		for _, status := range paths {
			if source := server.source(status.Name); source != nil {
				source.observePath(now, status)
			}
		}
	}
	if sessionErr != nil {
		return
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
	for _, session := range local {
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
			server.closeSession(session, true)
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
	recover := source.active && source.consumerCountLocked() > 0 && !lastActivity.IsZero() &&
		now.Sub(lastActivity) >= sourceStallTimeout &&
		(source.lastRecoveryAttemptAt.IsZero() || now.Sub(source.lastRecoveryAttemptAt) >= sourceRecoveryMinimumInterval)
	if recover {
		source.lastRecoveryAttemptAt = now
	}
	source.mu.Unlock()
	if bytesChanged && recordingID != "" {
		source.server.markMediaMTXRecordingActive(recordingID, now)
	}
	if recover {
		go source.recover()
	}
}

func (source *mediaMTXSource) recover() {
	source.lifecycleMu.Lock()
	defer source.lifecycleMu.Unlock()
	if source.server.isClosing() {
		return
	}
	ctx, cancel := context.WithTimeout(source.server.lifecycleContext, sourceControlRequestTimeout)
	defer cancel()
	active := true
	if _, _, _, err := callSourceControl(ctx, source.config.ControlSocket, sourceControlRequest{
		Operation: "set-active", Active: &active,
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
		})
		source.mu.Unlock()
	}
	sort.Slice(statuses, func(left, right int) bool { return statuses[left].ID < statuses[right].ID })
	return statuses
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
	if err := source.acquire(operationContext); err != nil {
		return Snapshot{}, fmt.Errorf("activate media source %q: %w", sourceID, err)
	}
	defer source.releasePending("")
	id, err := newSnapshotID()
	if err != nil {
		return Snapshot{}, err
	}
	response, jpeg, rgb, err := callSourceControl(operationContext, source.config.ControlSocket, sourceControlRequest{
		Operation: "snapshot", SnapshotID: id, IncludeRGB: request.IncludeRGB,
		RequestKeyframe: request.RequestKeyframe, RequireFresh: request.RequireFresh,
	})
	if err != nil {
		return Snapshot{}, fmt.Errorf("capture source snapshot: %w", err)
	}
	if response.SnapshotID != "" && response.SnapshotID != id {
		return Snapshot{}, errors.New("capture source returned a mismatched snapshot ID")
	}
	if response.Width != source.config.Width || response.Height != source.config.Height ||
		response.PixelFormat != "rgb8" {
		return Snapshot{}, errors.New("capture source snapshot metadata does not match the media source")
	}
	if request.includeRGB() {
		if response.RGBBytes != response.Width*response.Height*3 || len(rgb) != response.RGBBytes {
			return Snapshot{}, errors.New("capture source snapshot RGB does not match the media source")
		}
	} else if response.RGBBytes != 0 || len(rgb) != 0 {
		return Snapshot{}, errors.New("JPEG-only capture source returned forbidden RGB")
	}
	if len(response.CameraMatrix) != 9 || len(response.Distortion) < 4 {
		return Snapshot{}, errors.New("capture source snapshot does not contain camera intrinsics")
	}
	snapshot := Snapshot{
		ID: id, SourceID: source.config.ID, FrameID: response.FrameID,
		TimestampNanoseconds: response.TimestampNanoseconds,
		TimestampClockDomain: strings.ToLower(strings.TrimSpace(response.TimestampClockDomain)),
		Width:                response.Width, Height: response.Height, PixelFormat: response.PixelFormat,
		JPEG: jpeg, RGB: rgb, CameraMatrix: append([]float64(nil), response.CameraMatrix...),
		Distortion: append([]float64(nil), response.Distortion...),
		RenderPose: cloneSnapshotRenderPose(response.RenderPose), PoseFrameID: response.PoseFrameID,
		ExpiresAt: time.Now().Add(server.config.SnapshotTTL),
	}
	if snapshot.FrameID == "" {
		snapshot.FrameID = source.config.FrameID
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
	if snapshot.TimestampClockDomain == "unknown" && snapshot.TimestampNanoseconds == 0 {
		snapshot.TimestampNanoseconds = time.Now().UnixNano()
		snapshot.TimestampClockDomain = "system_realtime"
	}
	source.storeSnapshot(snapshot)
	return snapshot, nil
}

func (source *mediaMTXSource) storeSnapshot(snapshot Snapshot) {
	source.mu.Lock()
	defer source.mu.Unlock()
	now := time.Now()
	for id, current := range source.snapshots {
		if !current.ExpiresAt.After(now) {
			delete(source.snapshots, id)
		}
	}
	source.snapshots[snapshot.ID] = snapshot
	source.snapshotOrder = append(source.snapshotOrder, snapshot.ID)
	for len(source.snapshotOrder) > maximumSnapshots {
		oldest := source.snapshotOrder[0]
		source.snapshotOrder = source.snapshotOrder[1:]
		delete(source.snapshots, oldest)
	}
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
	var closeErr error
	server.closeOnce.Do(func() {
		server.mu.Lock()
		server.closing = true
		server.cancelLifecycle()
		server.mu.Unlock()
		close(server.closed)
		if server.httpServer != nil {
			closeErr = server.httpServer.close()
		}
		if server.listener != nil {
			if err := server.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) && closeErr == nil {
				closeErr = err
			}
		}
		server.operations.Wait()
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
		for _, session := range sessions {
			server.closeSession(session, true)
		}
		if err := server.stopAllMediaMTXRecordings(); err != nil && closeErr == nil {
			closeErr = err
		}
		for _, source := range sources {
			source.lifecycleMu.Lock()
			source.mu.Lock()
			source.cancelDeactivateTimerLocked()
			source.active = false
			source.activeSince = time.Time{}
			source.lastRecoveryAttemptAt = time.Time{}
			source.mu.Unlock()
			ctx, cancel := context.WithTimeout(context.Background(), sourceControlRequestTimeout)
			active := false
			if _, _, _, err := callSourceControl(ctx, source.config.ControlSocket, sourceControlRequest{
				Operation: "set-active", Active: &active,
			}); err != nil && closeErr == nil {
				closeErr = fmt.Errorf("deactivate media source %q: %w", source.config.ID, err)
			}
			cancel()
			source.lifecycleMu.Unlock()
		}
		if err := server.process.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	})
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
