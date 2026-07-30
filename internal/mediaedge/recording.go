package mediaedge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/pion/rtp"
)

const (
	recordingManifestSchema = "xgc.media-recording.v1"
	recordingContainer      = "matroska"
	recordingManifestName   = "manifest.json"
)

var (
	ErrRecordingDisabled    = errors.New("media recording is not configured")
	ErrRecordingNotFound    = errors.New("media recording was not found")
	ErrRecordingConflict    = errors.New("media source already has an active recording")
	ErrInsufficientCapacity = errors.New("insufficient recording capacity")
)

type RecordingState string

const (
	RecordingWaiting  RecordingState = "waiting-keyframe"
	RecordingActive   RecordingState = "recording"
	RecordingStopping RecordingState = "stopping"
	RecordingComplete RecordingState = "completed"
	RecordingFailed   RecordingState = "failed"
)

// StartRecordingRequest intentionally requires a bounded duration. Capacity
// admission cannot be honest without knowing how long the peak bitrate may be
// written.
type StartRecordingRequest struct {
	DurationSeconds int64 `json:"durationSeconds"`
}

type RecordingCodec struct {
	Name              string  `json:"name"`
	Container         string  `json:"container"`
	Width             int     `json:"width"`
	Height            int     `json:"height"`
	FPS               float64 `json:"fps"`
	RTPPayloadType    int     `json:"rtpPayloadType"`
	RTPClockRate      int     `json:"rtpClockRate"`
	SDPFmtpLine       string  `json:"sdpFmtpLine"`
	StreamCopy        bool    `json:"streamCopy"`
	MaxBitrateBitsSec uint64  `json:"maxBitrateBitsPerSecond"`
}

type RecordingQueueStatus struct {
	CapacityPackets  int    `json:"capacityPackets"`
	DepthPackets     int    `json:"depthPackets"`
	HighWaterPackets uint64 `json:"highWaterPackets"`
	OverflowPackets  uint64 `json:"overflowPackets"`
}

type RecordingSegment struct {
	Index             int       `json:"index"`
	Path              string    `json:"path"`
	FrameIndexPath    string    `json:"frameIndexPath"`
	StartedAt         time.Time `json:"startedAt"`
	EndedAt           time.Time `json:"endedAt"`
	StartRTPTimestamp uint32    `json:"startRtpTimestamp"`
	EndRTPTimestamp   uint32    `json:"endRtpTimestamp"`
	AccessUnits       uint64    `json:"accessUnits"`
	Keyframes         uint64    `json:"keyframes"`
	Bytes             uint64    `json:"bytes"`
	FrameIndexBytes   uint64    `json:"frameIndexBytes"`
	EndReason         string    `json:"endReason"`
}

// RecordingFrameIndexEntry preserves the rewritten RTP sample clock and Edge
// ingress time for every complete access unit. The Matroska timeline remains
// monotonic/playable; offline ROS/Gazebo correlation uses this sidecar instead
// of mistaking file-write time for camera acquisition time.
type RecordingFrameIndexEntry struct {
	AccessUnit        uint64    `json:"accessUnit"`
	SegmentAccessUnit uint64    `json:"segmentAccessUnit"`
	StartSequence     uint16    `json:"startRtpSequence"`
	EndSequence       uint16    `json:"endRtpSequence"`
	RTPTimestamp      uint32    `json:"rtpTimestamp"`
	SegmentPTS90k     uint32    `json:"segmentPts90k"`
	EdgeIngressTime   time.Time `json:"edgeIngressTime"`
	Keyframe          bool      `json:"keyframe"`
	AnnexBBytes       int       `json:"annexBBytes"`
}

// RecordingManifest is both the durable metadata contract and the status/list
// response. Paths are always relative to the configured recording root.
type RecordingManifest struct {
	SchemaVersion            string               `json:"schemaVersion"`
	RecordingID              string               `json:"recordingId"`
	SourceID                 string               `json:"sourceId"`
	State                    RecordingState       `json:"state"`
	CreatedAt                time.Time            `json:"createdAt"`
	CaptureStartedAt         *time.Time           `json:"captureStartedAt,omitempty"`
	StoppedAt                *time.Time           `json:"stoppedAt,omitempty"`
	RequestedDurationSeconds int64                `json:"requestedDurationSeconds"`
	Codec                    RecordingCodec       `json:"codec"`
	Queue                    RecordingQueueStatus `json:"queue"`
	PacketsObserved          uint64               `json:"packetsObserved"`
	PacketsQueued            uint64               `json:"packetsQueued"`
	RTPPacketsLost           uint64               `json:"rtpPacketsLost"`
	Discontinuities          uint64               `json:"discontinuities"`
	AccessUnitsDiscarded     uint64               `json:"accessUnitsDiscarded"`
	AccessUnitsWritten       uint64               `json:"accessUnitsWritten"`
	KeyframesWritten         uint64               `json:"keyframesWritten"`
	Bytes                    uint64               `json:"bytes"`
	FrameIndexBytes          uint64               `json:"frameIndexBytes"`
	Segments                 []RecordingSegment   `json:"segments"`
	FailureReason            string               `json:"failureReason,omitempty"`
}

type recordingPacket struct {
	packet             *rtp.Packet
	receivedAt         time.Time
	discontinuityEpoch uint64
}

type recording struct {
	server *Server
	source *source
	root   string
	path   string
	config RecordingConfig

	muxerFactory recordingMuxerFactory

	mu       sync.RWMutex
	manifest RecordingManifest

	ingestMu  sync.RWMutex
	accepting bool
	queue     chan recordingPacket
	done      chan struct{}

	stopOnce sync.Once

	discontinuityEpoch atomic.Uint64
	packetsObserved    atomic.Uint64
	packetsQueued      atomic.Uint64
	queueHighWater     atomic.Uint64
	queueOverflow      atomic.Uint64

	currentMuxer            recordingSegmentMuxer
	currentPartialPath      string
	currentFinalPath        string
	currentIndexFile        *os.File
	currentIndexPartialPath string
	currentIndexFinalPath   string
	currentSegment          RecordingSegment
	lastAccessUnitAt        time.Time

	assembler h264AccessUnitAssembler
	cachedSPS []byte
	cachedPPS []byte
}

func (server *Server) prepareRecording() error {
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
	ffmpegPath, err := exec.LookPath(config.FFmpegPath)
	if err != nil {
		return fmt.Errorf(
			"media recording requires FFmpeg for H264 stream-copy Matroska muxing; %q was not found: %w",
			config.FFmpegPath,
			err,
		)
	}
	config.Root = filepath.Clean(root)
	config.FFmpegPath = ffmpegPath
	server.config.Recording = config
	server.recordingMuxerFactory = newFFmpegSegmentMuxer(ffmpegPath)
	if err := server.loadRecordingHistory(); err != nil {
		return err
	}
	server.mu.Lock()
	server.recordingReady = true
	server.mu.Unlock()
	return nil
}

func (server *Server) loadRecordingHistory() error {
	entries, err := os.ReadDir(server.config.Recording.Root)
	if err != nil {
		return fmt.Errorf("list media recording root: %w", err)
	}
	history := make(map[string]RecordingManifest)
	for _, entry := range entries {
		if !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 || !stableSourceID.MatchString(entry.Name()) {
			continue
		}
		manifestPath, err := recordingPathWithinRoot(
			server.config.Recording.Root,
			entry.Name(),
			recordingManifestName,
		)
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
		if decodeErr != nil ||
			manifest.SchemaVersion != recordingManifestSchema ||
			manifest.RecordingID != entry.Name() {
			continue
		}
		switch manifest.State {
		case RecordingWaiting, RecordingActive, RecordingStopping:
			now := time.Now().UTC()
			manifest.State = RecordingFailed
			manifest.StoppedAt = &now
			if manifest.FailureReason == "" {
				manifest.FailureReason = "media edge restarted before recording finalized"
			}
			if err := writeRecordingManifest(server.config.Recording.Root, manifest); err != nil {
				return fmt.Errorf("recover interrupted media recording %q: %w", manifest.RecordingID, err)
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

func (server *Server) recordingAvailable() bool {
	server.mu.RLock()
	defer server.mu.RUnlock()
	return server.recordingReady && !server.closing
}

func (server *Server) StartRecording(
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
	if !server.recordingAvailable() {
		return RecordingManifest{}, errors.New("media recording is not ready")
	}
	if request.DurationSeconds < 1 {
		return RecordingManifest{}, errors.New("recording durationSeconds must be a positive integer")
	}
	maximumDurationSeconds := int64(server.config.Recording.MaxDuration / time.Second)
	if request.DurationSeconds > maximumDurationSeconds {
		return RecordingManifest{}, fmt.Errorf(
			"recording duration exceeds configured maximum of %s",
			server.config.Recording.MaxDuration,
		)
	}
	requestedDuration := time.Duration(request.DurationSeconds) * time.Second
	source := server.source(sourceID)
	if source == nil {
		return RecordingManifest{}, fmt.Errorf("media source %q was not found", sourceID)
	}

	source.recordingLifecycleMu.Lock()
	defer source.recordingLifecycleMu.Unlock()
	source.mu.Lock()
	conflict := source.recording != nil
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
	item := &recording{
		server: server, source: source, root: server.config.Recording.Root,
		path: recordingDirectory, config: server.config.Recording,
		muxerFactory: server.recordingMuxerFactory,
		accepting:    true,
		queue:        make(chan recordingPacket, server.config.Recording.QueuePackets),
		done:         make(chan struct{}),
		manifest: RecordingManifest{
			SchemaVersion:            recordingManifestSchema,
			RecordingID:              recordingID,
			SourceID:                 source.config.ID,
			State:                    RecordingWaiting,
			CreatedAt:                now,
			RequestedDurationSeconds: request.DurationSeconds,
			Codec: RecordingCodec{
				Name: sourceCodec, Container: recordingContainer,
				Width: source.config.Width, Height: source.config.Height, FPS: source.config.FPS,
				RTPPayloadType: sourceRTPPayloadType, RTPClockRate: h264RTPClockRate,
				SDPFmtpLine: h264FMTP, StreamCopy: true,
				MaxBitrateBitsSec: server.config.Recording.MaxBitrateBitsPerSecond,
			},
			Queue:    RecordingQueueStatus{CapacityPackets: server.config.Recording.QueuePackets},
			Segments: []RecordingSegment{},
		},
	}
	if err := item.persistManifest(); err != nil {
		return RecordingManifest{}, err
	}

	server.mu.Lock()
	if server.closing {
		server.mu.Unlock()
		return RecordingManifest{}, errors.New("media edge is closing")
	}
	server.recordings[recordingID] = item
	server.mu.Unlock()
	source.mu.Lock()
	if source.pending > 0 {
		source.pending--
	}
	source.recording = item
	source.mu.Unlock()
	pending = false
	keepDirectory = true

	go item.run(requestedDuration)
	source.requestKeyframeAsync(true)
	return item.status(), nil
}

func (server *Server) StopRecording(ctx context.Context, recordingID string) (RecordingManifest, error) {
	item := server.recording(recordingID)
	if item == nil {
		if manifest, found := server.recordingFromHistory(recordingID); found {
			return manifest, nil
		}
		return RecordingManifest{}, ErrRecordingNotFound
	}
	item.requestStop("")
	select {
	case <-item.done:
		return item.status(), nil
	case <-ctx.Done():
		return item.status(), fmt.Errorf("finalize media recording: %w", ctx.Err())
	}
}

func (server *Server) Recording(recordingID string) (RecordingManifest, bool) {
	item := server.recording(recordingID)
	if item != nil {
		return item.status(), true
	}
	return server.recordingFromHistory(recordingID)
}

func (server *Server) Recordings() []RecordingManifest {
	server.mu.RLock()
	active := make([]*recording, 0, len(server.recordings))
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
	sort.Slice(output, func(left int, right int) bool {
		return output[left].CreatedAt.After(output[right].CreatedAt)
	})
	return output
}

func (server *Server) recording(recordingID string) *recording {
	server.mu.RLock()
	defer server.mu.RUnlock()
	return server.recordings[recordingID]
}

func (server *Server) recordingFromHistory(recordingID string) (RecordingManifest, bool) {
	server.mu.RLock()
	defer server.mu.RUnlock()
	manifest, found := server.recordingHistory[recordingID]
	return manifest, found
}

func (recording *recording) enqueue(packet *rtp.Packet, receivedAt time.Time) {
	if recording == nil || packet == nil {
		return
	}
	recording.ingestMu.RLock()
	defer recording.ingestMu.RUnlock()
	if !recording.accepting {
		return
	}
	recording.packetsObserved.Add(1)
	if len(recording.queue) == cap(recording.queue) {
		recording.queueOverflow.Add(1)
		recording.discontinuityEpoch.Add(1)
		recording.source.requestKeyframeAsync(false)
		return
	}
	item := recordingPacket{
		packet: packet.Clone(), receivedAt: receivedAt.UTC(),
		discontinuityEpoch: recording.discontinuityEpoch.Load(),
	}
	select {
	case recording.queue <- item:
		recording.packetsQueued.Add(1)
		depth := uint64(len(recording.queue))
		for {
			current := recording.queueHighWater.Load()
			if depth <= current || recording.queueHighWater.CompareAndSwap(current, depth) {
				break
			}
		}
	default:
		// A writer can race between the capacity check and send. Preserve the
		// non-blocking boundary even though ingress currently has one producer.
		recording.queueOverflow.Add(1)
		recording.discontinuityEpoch.Add(1)
		recording.source.requestKeyframeAsync(false)
	}
}

func (recording *recording) markDiscontinuity() {
	if recording == nil {
		return
	}
	recording.discontinuityEpoch.Add(1)
	recording.source.requestKeyframeAsync(false)
}

func (recording *recording) requestStop(failure string) {
	if recording == nil {
		return
	}
	if failure != "" {
		recording.mu.Lock()
		if recording.manifest.FailureReason == "" {
			recording.manifest.FailureReason = failure
		}
		recording.mu.Unlock()
	}
	recording.stopOnce.Do(func() {
		recording.mu.Lock()
		if recording.manifest.State != RecordingFailed &&
			recording.manifest.State != RecordingComplete {
			recording.manifest.State = RecordingStopping
		}
		recording.mu.Unlock()
		recording.ingestMu.Lock()
		recording.accepting = false
		close(recording.queue)
		recording.ingestMu.Unlock()
	})
}

func (recording *recording) wait(ctx context.Context) error {
	select {
	case <-recording.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (recording *recording) run(requestedDuration time.Duration) {
	keyframeTimer := time.AfterFunc(recording.config.KeyframeTimeout, func() {
		recording.mu.RLock()
		started := recording.manifest.CaptureStartedAt != nil
		recording.mu.RUnlock()
		if !started {
			recording.requestStop(fmt.Sprintf(
				"no decodable SPS/PPS+IDR access unit arrived within %s",
				recording.config.KeyframeTimeout,
			))
		}
	})
	durationTimer := time.AfterFunc(requestedDuration, func() {
		recording.requestStop("")
	})
	defer keyframeTimer.Stop()
	defer durationTimer.Stop()
	defer func() {
		recording.finish()
		recording.source.recordingFinished(recording)
		close(recording.done)
	}()

	var lastEpoch uint64
	haveEpoch := false
	waitingForKeyframe := true
	for item := range recording.queue {
		if !haveEpoch {
			lastEpoch = item.discontinuityEpoch
			haveEpoch = true
		} else if item.discontinuityEpoch != lastEpoch {
			recording.recordDiscontinuity(0)
			if err := recording.finalizeCurrentSegment("queue-overflow-or-source-restart"); err != nil {
				recording.setFailure(err)
				break
			}
			recording.assembler.Reset()
			waitingForKeyframe = true
			lastEpoch = item.discontinuityEpoch
		}

		result := recording.assembler.Push(item.packet, item.receivedAt)
		if result.Discontinuity {
			recording.recordDiscontinuity(result.LostPackets)
			if err := recording.finalizeCurrentSegment("rtp-discontinuity"); err != nil {
				recording.setFailure(err)
				break
			}
			recording.assembler.Reset()
			waitingForKeyframe = true
			recording.source.requestKeyframeAsync(false)
		}
		accessUnit := result.AccessUnit
		if accessUnit == nil {
			continue
		}
		recording.cacheParameterSets(accessUnit)
		if waitingForKeyframe {
			if !accessUnit.Keyframe || len(recording.cachedSPS) == 0 || len(recording.cachedPPS) == 0 {
				recording.mu.Lock()
				recording.manifest.AccessUnitsDiscarded++
				recording.mu.Unlock()
				continue
			}
			if err := recording.openSegment(accessUnit); err != nil {
				recording.setFailure(err)
				break
			}
			waitingForKeyframe = false
			keyframeTimer.Stop()
		} else if accessUnit.Keyframe && recording.segmentDurationReached(accessUnit.Timestamp) {
			if err := recording.finalizeCurrentSegment("segment-duration"); err != nil {
				recording.setFailure(err)
				break
			}
			if err := recording.openSegment(accessUnit); err != nil {
				recording.setFailure(err)
				break
			}
		}
		if err := recording.writeAccessUnit(accessUnit); err != nil {
			recording.setFailure(err)
			break
		}
	}
	if recording.currentMuxer != nil {
		if err := recording.finalizeCurrentSegment("recording-stopped"); err != nil {
			recording.setFailure(err)
		}
	}
}

func (recording *recording) cacheParameterSets(accessUnit *h264AccessUnit) {
	for _, nalu := range accessUnit.NALUs {
		switch nalu[0] & 0x1f {
		case h264NALUTypeSPS:
			recording.cachedSPS = append(recording.cachedSPS[:0], nalu...)
		case h264NALUTypePPS:
			recording.cachedPPS = append(recording.cachedPPS[:0], nalu...)
		}
	}
}

func (recording *recording) openSegment(accessUnit *h264AccessUnit) error {
	recording.mu.RLock()
	index := len(recording.manifest.Segments) + 1
	recording.mu.RUnlock()
	name := fmt.Sprintf("segment-%06d.mkv", index)
	partialName := name + ".part"
	indexName := fmt.Sprintf("segment-%06d.frames.jsonl", index)
	indexPartialName := indexName + ".part"
	partialPath, err := recordingPathWithinRoot(recording.root, recording.manifest.RecordingID, "segments", partialName)
	if err != nil {
		return err
	}
	finalPath, err := recordingPathWithinRoot(recording.root, recording.manifest.RecordingID, "segments", name)
	if err != nil {
		return err
	}
	indexPartialPath, err := recordingPathWithinRoot(
		recording.root,
		recording.manifest.RecordingID,
		"segments",
		indexPartialName,
	)
	if err != nil {
		return err
	}
	indexFinalPath, err := recordingPathWithinRoot(
		recording.root,
		recording.manifest.RecordingID,
		"segments",
		indexName,
	)
	if err != nil {
		return err
	}
	muxer, err := recording.muxerFactory(partialPath, recording.source.config.FPS, recording.config.FinalizeTimeout)
	if err != nil {
		return err
	}
	indexFile, err := os.OpenFile(indexPartialPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o640)
	if err != nil {
		muxer.Abort()
		_ = os.Remove(partialPath)
		return fmt.Errorf("create recording frame index: %w", err)
	}
	recording.currentMuxer = muxer
	recording.currentPartialPath = partialPath
	recording.currentFinalPath = finalPath
	recording.currentIndexFile = indexFile
	recording.currentIndexPartialPath = indexPartialPath
	recording.currentIndexFinalPath = indexFinalPath
	recording.currentSegment = RecordingSegment{
		Index:             index,
		Path:              filepath.ToSlash(filepath.Join(recording.manifest.RecordingID, "segments", name)),
		FrameIndexPath:    filepath.ToSlash(filepath.Join(recording.manifest.RecordingID, "segments", indexName)),
		StartedAt:         accessUnit.ReceivedAt.UTC(),
		StartRTPTimestamp: accessUnit.Timestamp,
	}
	recording.lastAccessUnitAt = accessUnit.ReceivedAt.UTC()
	recording.mu.Lock()
	if recording.manifest.CaptureStartedAt == nil {
		started := accessUnit.ReceivedAt.UTC()
		recording.manifest.CaptureStartedAt = &started
	}
	recording.manifest.State = RecordingActive
	recording.mu.Unlock()
	return nil
}

func (recording *recording) writeAccessUnit(accessUnit *h264AccessUnit) error {
	if recording.currentMuxer == nil {
		return errors.New("recording segment muxer is not open")
	}
	var sps []byte
	var pps []byte
	if recording.currentSegment.AccessUnits == 0 {
		sps = recording.cachedSPS
		pps = recording.cachedPPS
	}
	data := accessUnitAnnexB(accessUnit, sps, pps)
	if len(data) == 0 {
		return errors.New("H264 access unit has no Annex-B payload")
	}
	if err := recording.currentMuxer.WriteAccessUnit(data); err != nil {
		recording.abortCurrentSegment()
		return err
	}
	recording.mu.RLock()
	globalAccessUnit := recording.manifest.AccessUnitsWritten + 1
	recording.mu.RUnlock()
	entry := RecordingFrameIndexEntry{
		AccessUnit: globalAccessUnit, SegmentAccessUnit: recording.currentSegment.AccessUnits + 1,
		StartSequence: accessUnit.StartSequence, EndSequence: accessUnit.EndSequence,
		RTPTimestamp:    accessUnit.Timestamp,
		SegmentPTS90k:   uint32(accessUnit.Timestamp - recording.currentSegment.StartRTPTimestamp),
		EdgeIngressTime: accessUnit.ReceivedAt.UTC(), Keyframe: accessUnit.Keyframe,
		AnnexBBytes: len(data),
	}
	if recording.currentIndexFile == nil {
		recording.abortCurrentSegment()
		return errors.New("recording frame index is not open")
	}
	if err := json.NewEncoder(recording.currentIndexFile).Encode(entry); err != nil {
		recording.abortCurrentSegment()
		return fmt.Errorf("write recording frame index: %w", err)
	}
	recording.currentSegment.AccessUnits++
	if accessUnit.Keyframe {
		recording.currentSegment.Keyframes++
	}
	recording.currentSegment.EndRTPTimestamp = accessUnit.Timestamp
	recording.lastAccessUnitAt = accessUnit.ReceivedAt.UTC()
	recording.mu.Lock()
	recording.manifest.AccessUnitsWritten++
	if accessUnit.Keyframe {
		recording.manifest.KeyframesWritten++
	}
	recording.mu.Unlock()
	return nil
}

func (recording *recording) abortCurrentSegment() {
	if recording.currentMuxer != nil {
		recording.currentMuxer.Abort()
		recording.currentMuxer = nil
	}
	if recording.currentIndexFile != nil {
		_ = recording.currentIndexFile.Close()
		recording.currentIndexFile = nil
	}
	_ = os.Remove(recording.currentPartialPath)
	_ = os.Remove(recording.currentIndexPartialPath)
	recording.currentPartialPath = ""
	recording.currentFinalPath = ""
	recording.currentIndexPartialPath = ""
	recording.currentIndexFinalPath = ""
}

func (recording *recording) segmentDurationReached(timestamp uint32) bool {
	if recording.currentMuxer == nil {
		return false
	}
	ticks := uint32(timestamp - recording.currentSegment.StartRTPTimestamp)
	return time.Duration(ticks)*time.Second/time.Duration(h264RTPClockRate) >= recording.config.SegmentDuration
}

func (recording *recording) finalizeCurrentSegment(reason string) error {
	if recording.currentMuxer == nil {
		return nil
	}
	muxer := recording.currentMuxer
	partialPath := recording.currentPartialPath
	finalPath := recording.currentFinalPath
	indexFile := recording.currentIndexFile
	indexPartialPath := recording.currentIndexPartialPath
	indexFinalPath := recording.currentIndexFinalPath
	recording.currentMuxer = nil
	recording.currentIndexFile = nil
	recording.currentPartialPath = ""
	recording.currentFinalPath = ""
	recording.currentIndexPartialPath = ""
	recording.currentIndexFinalPath = ""
	if indexFile == nil {
		muxer.Abort()
		_ = os.Remove(partialPath)
		return errors.New("recording frame index is not open")
	}
	if err := indexFile.Sync(); err != nil {
		_ = indexFile.Close()
		muxer.Abort()
		_ = os.Remove(partialPath)
		_ = os.Remove(indexPartialPath)
		return fmt.Errorf("sync recording frame index: %w", err)
	}
	if err := indexFile.Close(); err != nil {
		muxer.Abort()
		_ = os.Remove(partialPath)
		_ = os.Remove(indexPartialPath)
		return fmt.Errorf("close recording frame index: %w", err)
	}
	if err := muxer.Finalize(); err != nil {
		_ = os.Remove(partialPath)
		_ = os.Remove(indexPartialPath)
		return err
	}
	if err := syncAndRenameRecordingFile(partialPath, finalPath); err != nil {
		_ = os.Remove(partialPath)
		_ = os.Remove(indexPartialPath)
		return err
	}
	if err := syncAndRenameRecordingFile(indexPartialPath, indexFinalPath); err != nil {
		_ = os.Remove(indexPartialPath)
		_ = os.Remove(finalPath)
		return err
	}
	info, err := os.Stat(finalPath)
	if err != nil {
		_ = os.Remove(finalPath)
		_ = os.Remove(indexFinalPath)
		return fmt.Errorf("inspect finalized recording segment: %w", err)
	}
	indexInfo, err := os.Stat(indexFinalPath)
	if err != nil {
		_ = os.Remove(finalPath)
		_ = os.Remove(indexFinalPath)
		return fmt.Errorf("inspect finalized recording frame index: %w", err)
	}
	recording.currentSegment.EndedAt = recording.lastAccessUnitAt.UTC()
	recording.currentSegment.Bytes = uint64(info.Size())
	recording.currentSegment.FrameIndexBytes = uint64(indexInfo.Size())
	recording.currentSegment.EndReason = reason
	recording.mu.Lock()
	recording.manifest.Segments = append(recording.manifest.Segments, recording.currentSegment)
	recording.manifest.Bytes += recording.currentSegment.Bytes
	recording.manifest.FrameIndexBytes += recording.currentSegment.FrameIndexBytes
	recording.mu.Unlock()
	recording.currentSegment = RecordingSegment{}
	return recording.persistManifest()
}

func (recording *recording) recordDiscontinuity(lostPackets uint64) {
	recording.mu.Lock()
	recording.manifest.Discontinuities++
	recording.manifest.RTPPacketsLost += lostPackets
	recording.mu.Unlock()
}

func (recording *recording) setFailure(err error) {
	if err == nil {
		return
	}
	recording.mu.Lock()
	if recording.manifest.FailureReason == "" {
		recording.manifest.FailureReason = err.Error()
	}
	recording.mu.Unlock()
	recording.requestStop("")
}

func (recording *recording) finish() {
	recording.ingestMu.Lock()
	if recording.accepting {
		recording.accepting = false
		close(recording.queue)
	}
	recording.ingestMu.Unlock()
	if recording.currentMuxer != nil {
		recording.abortCurrentSegment()
	}
	now := time.Now().UTC()
	recording.mu.Lock()
	if recording.manifest.FailureReason == "" && len(recording.manifest.Segments) == 0 {
		recording.manifest.FailureReason = "recording ended before a decodable SPS/PPS+IDR access unit was received"
	}
	if recording.manifest.FailureReason == "" {
		recording.manifest.State = RecordingComplete
	} else {
		recording.manifest.State = RecordingFailed
	}
	recording.manifest.StoppedAt = &now
	recording.mu.Unlock()
	if err := recording.persistManifest(); err != nil {
		recording.mu.Lock()
		recording.manifest.State = RecordingFailed
		if recording.manifest.FailureReason == "" {
			recording.manifest.FailureReason = fmt.Sprintf("persist final recording manifest: %v", err)
		}
		recording.mu.Unlock()
		// Retry once so a transient rename/sync failure can still leave a
		// durable failed-state explanation.
		_ = recording.persistManifest()
	}
}

func (recording *recording) status() RecordingManifest {
	recording.mu.RLock()
	manifest := recording.manifest
	manifest.Segments = append([]RecordingSegment(nil), recording.manifest.Segments...)
	recording.mu.RUnlock()
	manifest.PacketsObserved = recording.packetsObserved.Load()
	manifest.PacketsQueued = recording.packetsQueued.Load()
	manifest.Queue.DepthPackets = len(recording.queue)
	manifest.Queue.HighWaterPackets = recording.queueHighWater.Load()
	manifest.Queue.OverflowPackets = recording.queueOverflow.Load()
	return manifest
}

func (recording *recording) persistManifest() error {
	manifest := recording.status()
	return writeRecordingManifest(recording.root, manifest)
}

func writeRecordingManifest(root string, manifest RecordingManifest) error {
	manifestPath, err := recordingPathWithinRoot(
		root,
		manifest.RecordingID,
		recordingManifestName,
	)
	if err != nil {
		return err
	}
	partialPath := manifestPath + ".part"
	content, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("encode recording manifest: %w", err)
	}
	content = append(content, '\n')
	file, err := os.OpenFile(partialPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o640)
	if err != nil {
		return fmt.Errorf("create recording manifest: %w", err)
	}
	if _, err := file.Write(content); err != nil {
		_ = file.Close()
		_ = os.Remove(partialPath)
		return fmt.Errorf("write recording manifest: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(partialPath)
		return fmt.Errorf("sync recording manifest: %w", err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(partialPath)
		return fmt.Errorf("close recording manifest: %w", err)
	}
	if err := os.Rename(partialPath, manifestPath); err != nil {
		_ = os.Remove(partialPath)
		return fmt.Errorf("atomically replace recording manifest: %w", err)
	}
	return syncDirectory(manifestPath)
}

func (source *source) recordingFinished(item *recording) {
	source.mu.Lock()
	if source.recording == item {
		source.recording = nil
	}
	unused := !source.hasConsumersLocked()
	source.mu.Unlock()
	if unused {
		source.scheduleDeactivate()
	}
	manifest := item.status()
	source.server.mu.Lock()
	delete(source.server.recordings, manifest.RecordingID)
	source.server.recordingHistory[manifest.RecordingID] = manifest
	source.server.mu.Unlock()
}

func createRecordingDirectory(root string, recordingID string) (string, error) {
	path, err := recordingPathWithinRoot(root, recordingID)
	if err != nil {
		return "", err
	}
	if err := os.Mkdir(path, 0o750); err != nil {
		return "", fmt.Errorf("create recording directory: %w", err)
	}
	segments, err := recordingPathWithinRoot(root, recordingID, "segments")
	if err != nil {
		_ = os.Remove(path)
		return "", err
	}
	if err := os.Mkdir(segments, 0o750); err != nil {
		_ = os.Remove(path)
		return "", fmt.Errorf("create recording segment directory: %w", err)
	}
	return path, nil
}

func recordingPathWithinRoot(root string, elements ...string) (string, error) {
	root = filepath.Clean(root)
	path := root
	for _, element := range elements {
		if element == "" || element == "." || element == ".." ||
			filepath.IsAbs(element) || filepath.Base(element) != element {
			return "", errors.New("recording path element is invalid")
		}
		path = filepath.Join(path, element)
	}
	relative, err := filepath.Rel(root, path)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", errors.New("recording path escapes configured root")
	}
	return path, nil
}

func ensureRecordingCapacity(config RecordingConfig, duration time.Duration) error {
	var filesystem syscall.Statfs_t
	if err := syscall.Statfs(config.Root, &filesystem); err != nil {
		return fmt.Errorf("inspect recording filesystem capacity: %w", err)
	}
	available := saturatingMultiply(uint64(filesystem.Bavail), uint64(filesystem.Bsize))
	seconds := duration.Seconds()
	estimated := float64(config.MaxBitrateBitsPerSecond) / 8 * seconds * config.CapacitySafetyFactor
	if math.IsInf(estimated, 0) || estimated > float64(math.MaxUint64) {
		return fmt.Errorf("%w: requested recording size overflows capacity calculation", ErrInsufficientCapacity)
	}
	requiredMedia := uint64(math.Ceil(estimated))
	required := saturatingAdd(requiredMedia, config.MinimumFreeBytes)
	if available < required {
		return fmt.Errorf(
			"%w: available=%d required=%d (media=%d reserve=%d)",
			ErrInsufficientCapacity,
			available,
			required,
			requiredMedia,
			config.MinimumFreeBytes,
		)
	}
	return nil
}

func saturatingMultiply(left uint64, right uint64) uint64 {
	if left != 0 && right > math.MaxUint64/left {
		return math.MaxUint64
	}
	return left * right
}

func saturatingAdd(left uint64, right uint64) uint64 {
	if right > math.MaxUint64-left {
		return math.MaxUint64
	}
	return left + right
}

func recordingHTTPStatus(err error) int {
	switch {
	case errors.Is(err, ErrRecordingNotFound):
		return http.StatusNotFound
	case errors.Is(err, ErrRecordingConflict):
		return http.StatusConflict
	case errors.Is(err, ErrInsufficientCapacity):
		return http.StatusInsufficientStorage
	case errors.Is(err, ErrRecordingDisabled):
		return http.StatusServiceUnavailable
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return http.StatusGatewayTimeout
	case strings.Contains(err.Error(), "was not found"):
		return http.StatusNotFound
	case strings.Contains(err.Error(), "activate media source"),
		strings.Contains(err.Error(), "not ready"):
		return http.StatusServiceUnavailable
	default:
		return http.StatusBadRequest
	}
}
