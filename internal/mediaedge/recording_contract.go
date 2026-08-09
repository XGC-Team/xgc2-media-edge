package mediaedge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const (
	recordingManifestSchema = "xgc.media-recording.v1"
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

// The queue and frame-index fields remain in the stable product manifest for
// compatibility. MediaMTX owns packet buffering and fMP4 muxing, so the XGC
// control layer leaves implementation-specific counters at zero.
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

// RecordingManifest is the stable product metadata/status contract. MediaMTX
// writes the native fMP4 segments; XGC owns lifecycle intent and this manifest.
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

func writeRecordingManifest(root string, manifest RecordingManifest) error {
	manifestPath, err := recordingPathWithinRoot(root, manifest.RecordingID, recordingManifestName)
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
	estimated := float64(config.MaxBitrateBitsPerSecond) / 8 * duration.Seconds() * config.CapacitySafetyFactor
	if math.IsInf(estimated, 0) || estimated > float64(math.MaxUint64) {
		return fmt.Errorf("%w: requested recording size overflows capacity calculation", ErrInsufficientCapacity)
	}
	requiredMedia := uint64(math.Ceil(estimated))
	required := saturatingAdd(requiredMedia, config.MinimumFreeBytes)
	if available < required {
		return fmt.Errorf("%w: available=%d required=%d (media=%d reserve=%d)", ErrInsufficientCapacity, available, required, requiredMedia, config.MinimumFreeBytes)
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
	case strings.Contains(err.Error(), "activate media source"), strings.Contains(err.Error(), "not ready"):
		return http.StatusServiceUnavailable
	default:
		return http.StatusBadRequest
	}
}

func syncDirectory(childPath string) error {
	directory, err := os.Open(filepath.Dir(childPath))
	if err != nil {
		return fmt.Errorf("open recording directory for sync: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync recording directory: %w", err)
	}
	return nil
}
