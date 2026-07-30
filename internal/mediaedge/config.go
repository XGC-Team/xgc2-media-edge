// Package mediaedge owns the target-local video data plane. It accepts only
// loopback RTP from registered capture sources and negotiates browser WebRTC
// sessions directly; Core is deliberately not on the media path.
package mediaedge

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pion/webrtc/v4"
)

var stableSourceID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

const (
	// ControlDataChannelLabel carries rare control messages such as an
	// immutable calibration snapshot. Live video always uses the RTP track.
	ControlDataChannelLabel = "xgc-media-control.v1"

	defaultSessionGrace = 10 * time.Second
	// Snapshots contain uncompressed RGB. At 4K one snapshot is roughly 24 MiB,
	// so these are intentionally short-lived calibration transactions, not a
	// second video buffer in the media edge.
	defaultSnapshotTTL = 15 * time.Second
	maximumSnapshots   = 2

	defaultRecordingQueuePackets   = 8192
	defaultRecordingSegment        = 5 * time.Minute
	defaultRecordingMaxDuration    = 24 * time.Hour
	defaultRecordingFinalize       = 15 * time.Second
	defaultRecordingKeyframeWait   = 8 * time.Second
	defaultRecordingMinimumFree    = uint64(1 << 30)
	defaultRecordingCapacityFactor = 1.20
)

// Config is the complete, target-local configuration of one media edge. The
// HTTP listener is loopback-only by default, but may be explicitly bound to a
// target interface for direct browser signaling. Browser media candidates are
// created by Pion and can use direct ICE or a configured TURN service.
type Config struct {
	ControlAddress       string
	AllowedOrigins       []string
	Sources              []SourceConfig
	ICEServers           []webrtc.ICEServer
	PublicIPs            []string
	SessionGracePeriod   time.Duration
	SnapshotTTL          time.Duration
	SessionGatherTimeout time.Duration
	Recording            RecordingConfig
}

// RecordingConfig enables optional, local H264 stream-copy recording. An empty
// Root disables the feature completely. When enabled, MaxBitrateBitsPerSecond
// is mandatory because capacity admission must use the source's configured
// peak bitrate rather than an optimistic observed average.
type RecordingConfig struct {
	Root                    string
	FFmpegPath              string
	MaxBitrateBitsPerSecond uint64
	QueuePackets            int
	SegmentDuration         time.Duration
	MaxDuration             time.Duration
	FinalizeTimeout         time.Duration
	KeyframeTimeout         time.Duration
	MinimumFreeBytes        uint64
	CapacitySafetyFactor    float64
}

// SourceConfig describes one locally produced, H264/RTP source. The capture
// source must send RTP only to RTPListenAddress and expose its describe,
// lifecycle, and snapshot endpoint through ControlSocket; neither is externally
// reachable. The four media metadata fields are optional expected values: all
// four must be omitted or all four must match the authoritative describe reply.
type SourceConfig struct {
	ID               string
	RTPListenAddress string
	ControlSocket    string
	Width            int
	Height           int
	FPS              float64
	FrameID          string
}

func (config Config) normalized() (Config, error) {
	config.ControlAddress = strings.TrimSpace(config.ControlAddress)
	if err := requireTCPAddress(config.ControlAddress); err != nil {
		return Config{}, fmt.Errorf("media edge control address: %w", err)
	}
	if len(config.Sources) == 0 {
		return Config{}, errors.New("media edge requires at least one source")
	}
	if config.SessionGracePeriod <= 0 {
		config.SessionGracePeriod = defaultSessionGrace
	}
	if config.SnapshotTTL <= 0 {
		config.SnapshotTTL = defaultSnapshotTTL
	}
	if config.SessionGatherTimeout <= 0 {
		config.SessionGatherTimeout = 12 * time.Second
	}
	recording, err := config.Recording.normalized()
	if err != nil {
		return Config{}, err
	}
	config.Recording = recording
	seen := make(map[string]struct{}, len(config.Sources))
	for index := range config.Sources {
		source, err := config.Sources[index].normalized()
		if err != nil {
			return Config{}, err
		}
		if _, duplicate := seen[source.ID]; duplicate {
			return Config{}, fmt.Errorf("media source %q is duplicated", source.ID)
		}
		seen[source.ID] = struct{}{}
		config.Sources[index] = source
	}
	for _, publicIP := range config.PublicIPs {
		if net.ParseIP(strings.TrimSpace(publicIP)) == nil {
			return Config{}, fmt.Errorf("media edge public IP %q is invalid", publicIP)
		}
	}
	origins := make([]string, 0, len(config.AllowedOrigins))
	seenOrigins := make(map[string]struct{}, len(config.AllowedOrigins))
	for _, value := range config.AllowedOrigins {
		origin, err := normalizeHTTPOrigin(value)
		if err != nil {
			return Config{}, fmt.Errorf("media edge allowed origin %q: %w", value, err)
		}
		if _, duplicate := seenOrigins[origin]; duplicate {
			continue
		}
		seenOrigins[origin] = struct{}{}
		origins = append(origins, origin)
	}
	config.AllowedOrigins = origins
	return config, nil
}

func (config RecordingConfig) enabled() bool {
	return strings.TrimSpace(config.Root) != ""
}

func (config RecordingConfig) normalized() (RecordingConfig, error) {
	config.Root = strings.TrimSpace(config.Root)
	config.FFmpegPath = strings.TrimSpace(config.FFmpegPath)
	if config.Root == "" {
		if config.FFmpegPath != "" ||
			config.MaxBitrateBitsPerSecond != 0 ||
			config.QueuePackets != 0 ||
			config.SegmentDuration != 0 ||
			config.MaxDuration != 0 ||
			config.FinalizeTimeout != 0 ||
			config.KeyframeTimeout != 0 ||
			config.MinimumFreeBytes != 0 ||
			config.CapacitySafetyFactor != 0 {
			return RecordingConfig{}, errors.New("media recording root is required when recording options are configured")
		}
		return RecordingConfig{}, nil
	}
	if !filepath.IsAbs(config.Root) {
		return RecordingConfig{}, errors.New("media recording root must be an absolute path")
	}
	config.Root = filepath.Clean(config.Root)
	if config.FFmpegPath == "" {
		config.FFmpegPath = "ffmpeg"
	}
	if config.MaxBitrateBitsPerSecond == 0 {
		return RecordingConfig{}, errors.New("media recording peak bitrate must be configured")
	}
	if config.MaxBitrateBitsPerSecond > 10_000_000_000 {
		return RecordingConfig{}, errors.New("media recording peak bitrate must not exceed 10 Gbit/s")
	}
	if config.QueuePackets == 0 {
		config.QueuePackets = defaultRecordingQueuePackets
	}
	if config.QueuePackets < 64 || config.QueuePackets > 131_072 {
		return RecordingConfig{}, errors.New("media recording queue must contain between 64 and 131072 RTP packets")
	}
	if config.SegmentDuration == 0 {
		config.SegmentDuration = defaultRecordingSegment
	}
	if config.SegmentDuration < time.Second || config.SegmentDuration > time.Hour {
		return RecordingConfig{}, errors.New("media recording segment duration must be between 1 second and 1 hour")
	}
	if config.MaxDuration == 0 {
		config.MaxDuration = defaultRecordingMaxDuration
	}
	if config.MaxDuration < time.Second || config.MaxDuration > 7*24*time.Hour {
		return RecordingConfig{}, errors.New("media recording maximum duration must be between 1 second and 7 days")
	}
	if config.FinalizeTimeout == 0 {
		config.FinalizeTimeout = defaultRecordingFinalize
	}
	if config.FinalizeTimeout < time.Second || config.FinalizeTimeout > time.Minute {
		return RecordingConfig{}, errors.New("media recording finalize timeout must be between 1 second and 1 minute")
	}
	if config.KeyframeTimeout == 0 {
		config.KeyframeTimeout = defaultRecordingKeyframeWait
	}
	if config.KeyframeTimeout < time.Second || config.KeyframeTimeout > time.Minute {
		return RecordingConfig{}, errors.New("media recording keyframe timeout must be between 1 second and 1 minute")
	}
	if config.MinimumFreeBytes == 0 {
		config.MinimumFreeBytes = defaultRecordingMinimumFree
	}
	if config.CapacitySafetyFactor == 0 {
		config.CapacitySafetyFactor = defaultRecordingCapacityFactor
	}
	if config.CapacitySafetyFactor < 1 || config.CapacitySafetyFactor > 10 {
		return RecordingConfig{}, errors.New("media recording capacity safety factor must be between 1 and 10")
	}
	return config, nil
}

func (config SourceConfig) normalized() (SourceConfig, error) {
	config.ID = strings.TrimSpace(config.ID)
	config.RTPListenAddress = strings.TrimSpace(config.RTPListenAddress)
	config.ControlSocket = strings.TrimSpace(config.ControlSocket)
	config.FrameID = strings.TrimSpace(config.FrameID)
	if !stableSourceID.MatchString(config.ID) {
		return SourceConfig{}, errors.New("media source ID must be a stable identifier")
	}
	if err := requireLoopbackUDP(config.RTPListenAddress); err != nil {
		return SourceConfig{}, fmt.Errorf("media source %q RTP listener: %w", config.ID, err)
	}
	if !strings.HasPrefix(config.ControlSocket, "/") {
		return SourceConfig{}, fmt.Errorf("media source %q control socket must be an absolute Unix path", config.ID)
	}
	if config.hasExpectedMetadata() {
		if config.Width < 16 || config.Height < 16 {
			return SourceConfig{}, fmt.Errorf(
				"media source %q expected width, height, FPS, and frame ID must all be provided; dimensions must be at least 16x16",
				config.ID,
			)
		}
		if config.FPS <= 0 || config.FPS > 240 || config.FrameID == "" {
			return SourceConfig{}, fmt.Errorf(
				"media source %q expected width, height, FPS, and frame ID must all be provided; FPS must be between 0 and 240",
				config.ID,
			)
		}
	}
	return config, nil
}

func (config SourceConfig) hasExpectedMetadata() bool {
	return config.Width != 0 || config.Height != 0 || config.FPS != 0 || config.FrameID != ""
}

func requireTCPAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil || strings.TrimSpace(port) == "" {
		return errors.New("must be a host:port value")
	}
	if strings.TrimSpace(host) == "" {
		return errors.New("must include an explicit host")
	}
	return nil
}

func requireLoopbackUDP(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil || strings.TrimSpace(port) == "" {
		return errors.New("must be a host:port value")
	}
	if err := requireLoopbackHost(host); err != nil {
		return err
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65_535 {
		return errors.New("port must be between 1 and 65535")
	}
	return nil
}

func requireLoopbackHost(host string) error {
	host = strings.TrimSpace(host)
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return errors.New("must bind a loopback host")
	}
	return nil
}

// normalizeHTTPOrigin accepts only serialized HTTP origins, never full URLs.
// Keeping the allowlist at origin granularity avoids accidentally authorizing
// credentials or an application path that browsers do not include in Origin.
func normalizeHTTPOrigin(value string) (string, error) {
	value = strings.TrimSpace(value)
	parsed, err := url.Parse(value)
	if err != nil {
		return "", errors.New("must be a valid URL origin")
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", errors.New("scheme must be http or https")
	}
	if parsed.Host == "" || parsed.Hostname() == "" {
		return "", errors.New("host is required")
	}
	if parsed.User != nil {
		return "", errors.New("credentials are not allowed")
	}
	if parsed.Opaque != "" ||
		(parsed.Path != "" && parsed.Path != "/") ||
		(parsed.RawPath != "" && parsed.RawPath != "/") {
		return "", errors.New("path is not allowed")
	}
	if parsed.RawQuery != "" || parsed.ForceQuery {
		return "", errors.New("query is not allowed")
	}
	if parsed.Fragment != "" {
		return "", errors.New("fragment is not allowed")
	}
	host := strings.ToLower(parsed.Hostname())
	port := parsed.Port()
	if port != "" {
		number, err := strconv.Atoi(port)
		if err != nil || number < 1 || number > 65_535 {
			return "", errors.New("port is invalid")
		}
		if (scheme == "http" && number == 80) || (scheme == "https" && number == 443) {
			port = ""
		} else {
			port = strconv.Itoa(number)
		}
	}
	if port != "" {
		host = net.JoinHostPort(host, port)
	} else if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	return scheme + "://" + host, nil
}
