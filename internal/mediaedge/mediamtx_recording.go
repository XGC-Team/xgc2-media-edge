package mediaedge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	mtx "github.com/lxk36/xgc2-media-edge/internal/mediamtx"
)

type mediaMTXRecording struct {
	server *MediaMTXServer
	source *mediaMTXSource
	root   string
	path   string

	mu       sync.RWMutex
	manifest RecordingManifest
	timer    *time.Timer
	stopOnce sync.Once
	done     chan struct{}
}

func (server *MediaMTXServer) prepareMediaMTXRecording() error {
	config := server.config.Recording
	if !config.enabled() {
		return nil
	}
	if err := os.MkdirAll(config.Root, 0o750); err != nil {
		return fmt.Errorf("create media recording root: %w", err)
	}
	root, err := filepath.EvalSymlinks(config.Root)
	if err != nil {
		return fmt.Errorf("resolve media recording root: %w", err)
	}
	root, err = filepath.Abs(root)
	if err != nil {
		return fmt.Errorf("make media recording root absolute: %w", err)
	}
	info, err := os.Stat(root)
	if err != nil {
		return fmt.Errorf("inspect media recording root: %w", err)
	}
	if !info.IsDir() {
		return errors.New("media recording root is not a directory")
	}
	server.config.Recording.Root = filepath.Clean(root)
	return server.loadMediaMTXRecordingHistory()
}

func (server *MediaMTXServer) loadMediaMTXRecordingHistory() error {
	entries, err := os.ReadDir(server.config.Recording.Root)
	if err != nil {
		return fmt.Errorf("list media recording root: %w", err)
	}
	history := make(map[string]RecordingManifest)
	for _, entry := range entries {
		if !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 || !stableSourceID.MatchString(entry.Name()) {
			continue
		}
		manifestPath, err := recordingPathWithinRoot(server.config.Recording.Root, entry.Name(), recordingManifestName)
		if err != nil {
			continue
		}
		info, err := os.Lstat(manifestPath)
		if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > 1<<20 {
			continue
		}
		file, err := os.Open(manifestPath)
		if err != nil {
			continue
		}
		var manifest RecordingManifest
		decodeErr := json.NewDecoder(io.LimitReader(file, 1<<20)).Decode(&manifest)
		_ = file.Close()
		if decodeErr != nil || manifest.SchemaVersion != recordingManifestSchema || manifest.RecordingID != entry.Name() {
			continue
		}
		switch manifest.State {
		case RecordingWaiting, RecordingActive, RecordingStopping:
			now := time.Now().UTC()
			manifest.State = RecordingFailed
			manifest.StoppedAt = &now
			manifest.FailureReason = "media edge restarted before MediaMTX recording finalized"
			if err := writeRecordingManifest(server.config.Recording.Root, manifest); err != nil {
				return fmt.Errorf("recover interrupted MediaMTX recording %q: %w", manifest.RecordingID, err)
			}
		}
		history[manifest.RecordingID] = manifest
	}
	server.mu.Lock()
	for id, manifest := range history {
		server.recordingHistory[id] = manifest
	}
	server.mu.Unlock()
	return nil
}

func (server *MediaMTXServer) StartRecording(
	ctx context.Context,
	sourceID string,
	request StartRecordingRequest,
) (RecordingManifest, error) {
	operationContext, finishOperation, err := server.beginOperation(ctx)
	if err != nil {
		return RecordingManifest{}, err
	}
	defer finishOperation()
	if !server.config.Recording.enabled() {
		return RecordingManifest{}, ErrRecordingDisabled
	}
	if request.DurationSeconds < 1 {
		return RecordingManifest{}, errors.New("recording durationSeconds must be a positive integer")
	}
	maximumDurationSeconds := int64(server.config.Recording.MaxDuration / time.Second)
	if request.DurationSeconds > maximumDurationSeconds {
		return RecordingManifest{}, fmt.Errorf("recording duration exceeds configured maximum of %s", server.config.Recording.MaxDuration)
	}
	source := server.source(sourceID)
	if source == nil {
		return RecordingManifest{}, fmt.Errorf("media source %q was not found", sourceID)
	}
	requestedDuration := time.Duration(request.DurationSeconds) * time.Second
	source.recordingLifecycleMu.Lock()
	defer source.recordingLifecycleMu.Unlock()
	source.mu.Lock()
	conflict := source.recordingID != ""
	source.mu.Unlock()
	if conflict {
		return RecordingManifest{}, ErrRecordingConflict
	}
	if err := ensureRecordingCapacity(server.config.Recording, requestedDuration); err != nil {
		return RecordingManifest{}, err
	}
	if err := source.acquire(operationContext); err != nil {
		return RecordingManifest{}, fmt.Errorf("activate media source %q: %w", sourceID, err)
	}
	pending := true
	defer func() {
		if pending {
			source.releasePending("")
		}
	}()
	source.requestKeyframeAsync(true)
	if err := server.waitForMediaMTXPath(operationContext, sourceID); err != nil {
		return RecordingManifest{}, fmt.Errorf("activate media source %q for recording: %w", sourceID, err)
	}
	recordingID, err := newSnapshotID()
	if err != nil {
		return RecordingManifest{}, err
	}
	recordingDirectory, err := createRecordingDirectory(server.config.Recording.Root, recordingID)
	if err != nil {
		return RecordingManifest{}, err
	}
	keepDirectory := false
	defer func() {
		if !keepDirectory {
			_ = os.Remove(filepath.Join(recordingDirectory, "segments"))
			_ = os.Remove(recordingDirectory)
		}
	}()
	now := time.Now().UTC()
	item := &mediaMTXRecording{
		server: server, source: source, root: server.config.Recording.Root, path: recordingDirectory,
		done: make(chan struct{}),
		manifest: RecordingManifest{
			SchemaVersion: recordingManifestSchema, RecordingID: recordingID, SourceID: source.config.ID,
			State: RecordingWaiting, CreatedAt: now, RequestedDurationSeconds: request.DurationSeconds,
			Codec: RecordingCodec{
				Name: sourceCodec, Container: "fmp4", Width: source.config.Width, Height: source.config.Height,
				FPS: source.config.FPS, RTPPayloadType: sourceRTPPayloadType, RTPClockRate: h264RTPClockRate,
				SDPFmtpLine: h264FMTP, StreamCopy: true,
				MaxBitrateBitsSec: server.config.Recording.MaxBitrateBitsPerSecond,
			},
			Segments: []RecordingSegment{},
		},
	}
	if err := item.persist(); err != nil {
		return RecordingManifest{}, err
	}
	recordPath := filepath.ToSlash(filepath.Join(recordingDirectory, "segments", "%path-segment-%Y-%m-%d_%H-%M-%S-%f"))
	if err := server.control.ConfigureRecording(operationContext, sourceID, mtx.RecordingSettings{
		Enabled: true, Path: recordPath, PartDuration: "1s",
		SegmentDuration: durationString(server.config.Recording.SegmentDuration),
	}); err != nil {
		return RecordingManifest{}, fmt.Errorf("enable MediaMTX recording: %w", err)
	}
	server.mu.Lock()
	if server.closing {
		server.mu.Unlock()
		_ = server.control.SetRecording(context.Background(), sourceID, false)
		return RecordingManifest{}, errors.New("media edge is closing")
	}
	server.recordings[recordingID] = item
	server.mu.Unlock()
	source.mu.Lock()
	if source.pending > 0 {
		source.pending--
	}
	source.recordingID = recordingID
	source.mu.Unlock()
	pending = false
	keepDirectory = true
	item.timer = time.AfterFunc(requestedDuration, func() { item.requestStop("") })
	source.requestKeyframeAsync(true)
	return item.status(), nil
}

func (server *MediaMTXServer) StopRecording(ctx context.Context, recordingID string) (RecordingManifest, error) {
	server.mu.RLock()
	item := server.recordings[recordingID]
	history, inHistory := server.recordingHistory[recordingID]
	server.mu.RUnlock()
	if item == nil {
		if inHistory {
			return history, nil
		}
		return RecordingManifest{}, ErrRecordingNotFound
	}
	item.requestStop("")
	select {
	case <-item.done:
		return item.status(), nil
	case <-ctx.Done():
		return item.status(), fmt.Errorf("finalize MediaMTX recording: %w", ctx.Err())
	}
}

func (server *MediaMTXServer) Recording(recordingID string) (RecordingManifest, bool) {
	server.mu.RLock()
	item := server.recordings[recordingID]
	history, found := server.recordingHistory[recordingID]
	server.mu.RUnlock()
	if item != nil {
		return item.status(), true
	}
	return history, found
}

func (server *MediaMTXServer) Recordings() []RecordingManifest {
	server.mu.RLock()
	active := make([]*mediaMTXRecording, 0, len(server.recordings))
	for _, item := range server.recordings {
		active = append(active, item)
	}
	history := make(map[string]RecordingManifest, len(server.recordingHistory))
	for id, manifest := range server.recordingHistory {
		history[id] = manifest
	}
	server.mu.RUnlock()
	output := make([]RecordingManifest, 0, len(active)+len(history))
	for _, item := range active {
		manifest := item.status()
		output = append(output, manifest)
		delete(history, manifest.RecordingID)
	}
	for _, manifest := range history {
		output = append(output, manifest)
	}
	sort.Slice(output, func(left, right int) bool { return output[left].CreatedAt.After(output[right].CreatedAt) })
	return output
}

func (item *mediaMTXRecording) requestStop(failure string) {
	if failure != "" {
		item.mu.Lock()
		if item.manifest.FailureReason == "" {
			item.manifest.FailureReason = failure
		}
		item.mu.Unlock()
	}
	item.stopOnce.Do(func() {
		if item.timer != nil {
			item.timer.Stop()
		}
		go item.finalize()
	})
}

func (item *mediaMTXRecording) finalize() {
	defer close(item.done)
	item.mu.Lock()
	item.manifest.State = RecordingStopping
	item.mu.Unlock()
	_ = item.persist()
	ctx, cancel := context.WithTimeout(context.Background(), mediaMTXSessionCloseTimeout+3*time.Second)
	err := item.server.control.SetRecording(ctx, item.source.config.ID, false)
	cancel()
	if err != nil {
		item.mu.Lock()
		item.manifest.FailureReason = fmt.Sprintf("disable MediaMTX recording: %v", err)
		item.mu.Unlock()
	}
	segments, scanErr := item.waitForFinalizedSegments()
	if scanErr != nil {
		item.mu.Lock()
		if item.manifest.FailureReason == "" {
			item.manifest.FailureReason = scanErr.Error()
		}
		item.mu.Unlock()
	}
	now := time.Now().UTC()
	item.mu.Lock()
	item.manifest.Segments = segments
	item.manifest.Bytes = 0
	for _, segment := range segments {
		item.manifest.Bytes += segment.Bytes
	}
	if len(segments) == 0 && item.manifest.FailureReason == "" {
		item.manifest.FailureReason = "recording ended before MediaMTX finalized an fMP4 segment"
	}
	if item.manifest.FailureReason == "" {
		item.manifest.State = RecordingComplete
	} else {
		item.manifest.State = RecordingFailed
	}
	item.manifest.StoppedAt = &now
	item.mu.Unlock()
	if err := item.persist(); err != nil {
		item.mu.Lock()
		item.manifest.State = RecordingFailed
		if item.manifest.FailureReason == "" {
			item.manifest.FailureReason = fmt.Sprintf("persist final recording manifest: %v", err)
		}
		item.mu.Unlock()
		_ = item.persist()
	}
	item.source.recordingLifecycleMu.Lock()
	item.source.mu.Lock()
	if item.source.recordingID == item.manifest.RecordingID {
		item.source.recordingID = ""
	}
	unused := !item.source.hasConsumersLocked()
	item.source.mu.Unlock()
	item.source.recordingLifecycleMu.Unlock()
	if unused {
		item.source.scheduleDeactivate()
	}
	manifest := item.status()
	item.server.mu.Lock()
	delete(item.server.recordings, manifest.RecordingID)
	item.server.recordingHistory[manifest.RecordingID] = manifest
	item.server.mu.Unlock()
}

func (item *mediaMTXRecording) markActive(at time.Time) {
	item.mu.Lock()
	if item.manifest.State == RecordingWaiting {
		started := at.UTC()
		item.manifest.State = RecordingActive
		item.manifest.CaptureStartedAt = &started
	}
	item.mu.Unlock()
}

func (item *mediaMTXRecording) scanSegments() ([]RecordingSegment, error) {
	directory := filepath.Join(item.path, "segments")
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, fmt.Errorf("list MediaMTX recording segments: %w", err)
	}
	segments := make([]RecordingSegment, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 || !strings.HasSuffix(strings.ToLower(entry.Name()), ".mp4") {
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		segments = append(segments, RecordingSegment{
			Path:    filepath.ToSlash(filepath.Join(item.manifest.RecordingID, "segments", entry.Name())),
			EndedAt: info.ModTime().UTC(), Bytes: uint64(info.Size()),
		})
	}
	sort.Slice(segments, func(left, right int) bool { return segments[left].Path < segments[right].Path })
	started := item.manifest.CreatedAt
	for index := range segments {
		segments[index].Index = index + 1
		segments[index].StartedAt = started
		segments[index].EndReason = "segment-duration"
		started = segments[index].EndedAt
	}
	if len(segments) > 0 {
		segments[len(segments)-1].EndReason = "recording-stopped"
	}
	return segments, nil
}

func (item *mediaMTXRecording) waitForFinalizedSegments() ([]RecordingSegment, error) {
	timeout := item.server.config.Recording.FinalizeTimeout
	if timeout <= 0 {
		timeout = defaultRecordingFinalize
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var latest []RecordingSegment
	lastSignature := ""
	stableObservations := 0
	for {
		segments, err := item.scanSegments()
		if err != nil {
			return nil, err
		}
		latest = segments
		signature := mediaMTXSegmentSignature(segments)
		if len(segments) > 0 && signature == lastSignature {
			stableObservations++
			if stableObservations >= 3 {
				return segments, nil
			}
		} else {
			lastSignature = signature
			stableObservations = 1
		}
		select {
		case <-deadline.C:
			if len(latest) > 0 {
				return latest, nil
			}
			return nil, errors.New("MediaMTX did not finalize an fMP4 segment before the recording timeout")
		case <-ticker.C:
		}
	}
}

func mediaMTXSegmentSignature(segments []RecordingSegment) string {
	var signature strings.Builder
	for _, segment := range segments {
		_, _ = fmt.Fprintf(&signature, "%s:%d:%d;", segment.Path, segment.Bytes, segment.EndedAt.UnixNano())
	}
	return signature.String()
}

func (item *mediaMTXRecording) status() RecordingManifest {
	item.mu.RLock()
	manifest := item.manifest
	manifest.Segments = append([]RecordingSegment(nil), item.manifest.Segments...)
	item.mu.RUnlock()
	return manifest
}

func (item *mediaMTXRecording) persist() error {
	return writeRecordingManifest(item.root, item.status())
}

func (server *MediaMTXServer) stopAllMediaMTXRecordings() error {
	server.mu.RLock()
	items := make([]*mediaMTXRecording, 0, len(server.recordings))
	for _, item := range server.recordings {
		items = append(items, item)
	}
	server.mu.RUnlock()
	for _, item := range items {
		item.requestStop("media edge Session stopped")
	}
	var firstErr error
	for _, item := range items {
		select {
		case <-item.done:
		case <-time.After(server.config.Recording.FinalizeTimeout + 2*time.Second):
			if firstErr == nil {
				firstErr = fmt.Errorf("finalize MediaMTX recording %q: timeout", item.manifest.RecordingID)
			}
		}
	}
	return firstErr
}

func (server *MediaMTXServer) markMediaMTXRecordingActive(recordingID string, at time.Time) {
	server.mu.RLock()
	item := server.recordings[recordingID]
	server.mu.RUnlock()
	if item != nil {
		item.markActive(at)
	}
}

func durationString(value time.Duration) string {
	if value <= 0 {
		value = defaultRecordingSegment
	}
	return value.String()
}
