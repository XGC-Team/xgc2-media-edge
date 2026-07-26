// Package mediaedge owns the target-local video data plane. It accepts only
// loopback RTP from registered capture sources and negotiates browser WebRTC
// sessions directly; Core is deliberately not on the media path.
package mediaedge

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/pion/webrtc/v4"
)

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
)

// Config is the complete, target-local configuration of one media edge. The
// TCP control listener is loopback-only. Browser media candidates are created
// by Pion and can use direct ICE or a configured TURN service.
type Config struct {
	ControlAddress       string
	Sources              []SourceConfig
	ICEServers           []webrtc.ICEServer
	PublicIPs            []string
	SessionGracePeriod   time.Duration
	SnapshotTTL          time.Duration
	SessionGatherTimeout time.Duration
}

// SourceConfig describes one locally produced, H264/RTP source. The capture
// source must send RTP only to RTPListenAddress and expose its lifecycle and
// snapshot endpoint through ControlSocket; neither is externally reachable.
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
	if err := requireLoopbackTCP(config.ControlAddress); err != nil {
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
	return config, nil
}

func (config SourceConfig) normalized() (SourceConfig, error) {
	config.ID = strings.TrimSpace(config.ID)
	config.RTPListenAddress = strings.TrimSpace(config.RTPListenAddress)
	config.ControlSocket = strings.TrimSpace(config.ControlSocket)
	config.FrameID = strings.TrimSpace(config.FrameID)
	if config.ID == "" {
		return SourceConfig{}, errors.New("media source ID is required")
	}
	if err := requireLoopbackUDP(config.RTPListenAddress); err != nil {
		return SourceConfig{}, fmt.Errorf("media source %q RTP listener: %w", config.ID, err)
	}
	if !strings.HasPrefix(config.ControlSocket, "/") {
		return SourceConfig{}, fmt.Errorf("media source %q control socket must be an absolute Unix path", config.ID)
	}
	if config.Width < 16 || config.Height < 16 {
		return SourceConfig{}, fmt.Errorf("media source %q dimensions must be at least 16x16", config.ID)
	}
	if config.FPS <= 0 || config.FPS > 240 {
		return SourceConfig{}, fmt.Errorf("media source %q FPS must be between 0 and 240", config.ID)
	}
	return config, nil
}

func requireLoopbackTCP(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil || strings.TrimSpace(port) == "" {
		return errors.New("must be a host:port value")
	}
	return requireLoopbackHost(host)
}

func requireLoopbackUDP(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil || strings.TrimSpace(port) == "" {
		return errors.New("must be a host:port value")
	}
	return requireLoopbackHost(host)
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
