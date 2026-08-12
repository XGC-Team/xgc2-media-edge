// Package mediamtx contains the deliberately small integration boundary
// between the XGC product lifecycle and the upstream MediaMTX media server.
// It does not implement codecs, RTP fanout, WebRTC, or recording containers.
package mediamtx

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"regexp"
	"sort"
	"strings"
)

const (
	// Version is intentionally pinned. Packaging verifies the corresponding
	// upstream artifact checksum before it is installed beside the XGC wrapper.
	Version = "v1.20.0"

	LinuxAMD64SHA256 = "952d5f7d31d1b448ab4da4509550594c511d42636db9d7bb175d377f4ede81df"
	LinuxARM64SHA256 = "6aa3c03da7b6477f1e110c8e18e819cf9ef121e8981b52b8f8219982dae35f2f"

	// A 4K H264 IDR can exceed the Linux default UDP receive buffer in one
	// burst. MediaMTX must absorb the complete FU-A sequence or the browser
	// receives an undecodable frame and stalls until a later IDR.
	udpReadBufferSize = 4 * 1024 * 1024
)

var pathName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

// Config exposes only the MediaMTX knobs owned by the XGC media product. All
// unrelated ingest and serving protocols are explicitly disabled in Render.
type Config struct {
	APIAddress        string
	WHEPAddress       string
	ICEUDPAddress     string
	ICETCPAddress     string
	AllowedOrigins    []string
	AdditionalHosts   []string
	ICEServers        []ICEServer
	IPsFromInterfaces bool
	Paths             []Path
}

type ICEServer struct {
	URL        string `json:"url"`
	Username   string `json:"username,omitempty"`
	Password   string `json:"password,omitempty"`
	ClientOnly bool   `json:"clientOnly"`
}

// Path is one H264/PT96/90k RTP ingress owned by a co-located XGC adapter.
// RecordPath is configured up front, while recording remains disabled until
// the XGC lifecycle API explicitly leases it.
type Path struct {
	Name       string
	RTPAddress string
	RecordPath string
}

type renderedConfig struct {
	LogLevel                string                  `json:"logLevel"`
	LogDestinations         []string                `json:"logDestinations"`
	UDPReadBufferSize       int                     `json:"udpReadBufferSize"`
	API                     bool                    `json:"api"`
	APIAddress              string                  `json:"apiAddress"`
	APIEncryption           bool                    `json:"apiEncryption"`
	Metrics                 bool                    `json:"metrics"`
	PPROF                   bool                    `json:"pprof"`
	Playback                bool                    `json:"playback"`
	RTSP                    bool                    `json:"rtsp"`
	RTMP                    bool                    `json:"rtmp"`
	HLS                     bool                    `json:"hls"`
	WebRTC                  bool                    `json:"webrtc"`
	WebRTCAddress           string                  `json:"webrtcAddress"`
	WebRTCEncryption        bool                    `json:"webrtcEncryption"`
	WebRTCAllowOrigins      []string                `json:"webrtcAllowOrigins"`
	WebRTCLocalUDPAddress   string                  `json:"webrtcLocalUDPAddress"`
	WebRTCLocalTCPAddress   string                  `json:"webrtcLocalTCPAddress"`
	WebRTCIPsFromInterfaces bool                    `json:"webrtcIPsFromInterfaces"`
	WebRTCAdditionalHosts   []string                `json:"webrtcAdditionalHosts"`
	WebRTCICEServers        []ICEServer             `json:"webrtcICEServers2"`
	SRT                     bool                    `json:"srt"`
	MoQ                     bool                    `json:"moq"`
	Paths                   map[string]renderedPath `json:"paths"`
}

type renderedPath struct {
	Source                string `json:"source"`
	RTPSDP                string `json:"rtpSDP"`
	SourceOnDemand        bool   `json:"sourceOnDemand"`
	Record                bool   `json:"record"`
	RecordPath            string `json:"recordPath,omitempty"`
	RecordFormat          string `json:"recordFormat,omitempty"`
	RecordPartDuration    string `json:"recordPartDuration,omitempty"`
	RecordSegmentDuration string `json:"recordSegmentDuration,omitempty"`
}

// Render returns JSON, which is valid YAML and is accepted by MediaMTX. Using
// encoding/json avoids a shell-template or YAML-injection boundary for source
// IDs, addresses, origins, and recording paths.
func Render(config Config) ([]byte, error) {
	if err := validateTCPAddress(config.APIAddress, true); err != nil {
		return nil, fmt.Errorf("MediaMTX API address: %w", err)
	}
	if err := validateTCPAddress(config.WHEPAddress, true); err != nil {
		return nil, fmt.Errorf("MediaMTX WHEP address: %w", err)
	}
	if err := validateUDPAddress(config.ICEUDPAddress); err != nil {
		return nil, fmt.Errorf("MediaMTX ICE UDP address: %w", err)
	}
	if config.ICETCPAddress != "" {
		if err := validateTCPAddress(config.ICETCPAddress, false); err != nil {
			return nil, fmt.Errorf("MediaMTX ICE TCP address: %w", err)
		}
	}
	if len(config.Paths) == 0 {
		return nil, errors.New("MediaMTX requires at least one configured path")
	}

	origins := uniqueSorted(config.AllowedOrigins)
	if len(origins) == 0 {
		// WHEP is proxied by the XGC wrapper, so there is no reason to expose the
		// internal listener to an arbitrary browser Origin.
		origins = []string{"http://127.0.0.1"}
	}
	hosts := uniqueSorted(config.AdditionalHosts)
	for _, host := range hosts {
		if strings.TrimSpace(host) == "" || strings.ContainsAny(host, "/?#") {
			return nil, fmt.Errorf("MediaMTX additional host %q is invalid", host)
		}
	}
	iceServers := append([]ICEServer{}, config.ICEServers...)
	for index := range iceServers {
		iceServers[index].URL = strings.TrimSpace(iceServers[index].URL)
		iceServers[index].Username = strings.TrimSpace(iceServers[index].Username)
		if iceServers[index].URL == "" || strings.ContainsAny(iceServers[index].URL, "\r\n") {
			return nil, fmt.Errorf("MediaMTX ICE server URL %q is invalid", iceServers[index].URL)
		}
	}

	paths := make(map[string]renderedPath, len(config.Paths))
	for _, item := range config.Paths {
		item.Name = strings.TrimSpace(item.Name)
		item.RTPAddress = strings.TrimSpace(item.RTPAddress)
		item.RecordPath = strings.TrimSpace(item.RecordPath)
		if !pathName.MatchString(item.Name) {
			return nil, fmt.Errorf("MediaMTX path name %q is invalid", item.Name)
		}
		if _, duplicate := paths[item.Name]; duplicate {
			return nil, fmt.Errorf("MediaMTX path %q is duplicated", item.Name)
		}
		if err := validateLoopbackUDPAddress(item.RTPAddress); err != nil {
			return nil, fmt.Errorf("MediaMTX path %q RTP address: %w", item.Name, err)
		}
		path := renderedPath{
			Source:         "udp+rtp://" + item.RTPAddress,
			RTPSDP:         h264SDP(item.Name, item.RTPAddress),
			SourceOnDemand: false,
			Record:         false,
		}
		if item.RecordPath != "" {
			if !strings.HasPrefix(item.RecordPath, "/") {
				return nil, fmt.Errorf("MediaMTX path %q recording path must be absolute", item.Name)
			}
			path.RecordPath = item.RecordPath
			path.RecordFormat = "fmp4"
			path.RecordPartDuration = "1s"
			path.RecordSegmentDuration = "5m"
		}
		paths[item.Name] = path
	}

	output := renderedConfig{
		LogLevel:                "info",
		LogDestinations:         []string{"stdout"},
		UDPReadBufferSize:       udpReadBufferSize,
		API:                     true,
		APIAddress:              config.APIAddress,
		APIEncryption:           false,
		Metrics:                 false,
		PPROF:                   false,
		Playback:                false,
		RTSP:                    false,
		RTMP:                    false,
		HLS:                     false,
		WebRTC:                  true,
		WebRTCAddress:           config.WHEPAddress,
		WebRTCEncryption:        false,
		WebRTCAllowOrigins:      origins,
		WebRTCLocalUDPAddress:   config.ICEUDPAddress,
		WebRTCLocalTCPAddress:   config.ICETCPAddress,
		WebRTCIPsFromInterfaces: config.IPsFromInterfaces,
		WebRTCAdditionalHosts:   hosts,
		WebRTCICEServers:        iceServers,
		SRT:                     false,
		MoQ:                     false,
		Paths:                   paths,
	}
	return json.MarshalIndent(output, "", "  ")
}

func h264SDP(name string, address string) string {
	host, port, _ := net.SplitHostPort(address)
	if ip := net.ParseIP(host); ip != nil && ip.To4() == nil {
		return fmt.Sprintf("v=0\r\no=- 0 0 IN IP6 %s\r\ns=XGC %s\r\nc=IN IP6 %s\r\nt=0 0\r\nm=video %s RTP/AVP 96\r\na=rtpmap:96 H264/90000\r\na=fmtp:96 packetization-mode=1\r\na=recvonly\r\n", host, name, host, port)
	}
	return fmt.Sprintf("v=0\r\no=- 0 0 IN IP4 %s\r\ns=XGC %s\r\nc=IN IP4 %s\r\nt=0 0\r\nm=video %s RTP/AVP 96\r\na=rtpmap:96 H264/90000\r\na=fmtp:96 packetization-mode=1\r\na=recvonly\r\n", host, name, host, port)
}

func uniqueSorted(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, found := seen[value]; found {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func validateTCPAddress(address string, requireLoopback bool) error {
	return validateAddress(address, requireLoopback)
}

func validateUDPAddress(address string) error {
	return validateAddress(address, false)
}

func validateLoopbackUDPAddress(address string) error {
	return validateAddress(address, true)
}

func validateAddress(address string, requireLoopback bool) error {
	host, port, err := net.SplitHostPort(strings.TrimSpace(address))
	if err != nil || port == "" {
		return errors.New("must be a host:port value")
	}
	if host == "" {
		return errors.New("must include an explicit host")
	}
	if requireLoopback {
		ip := net.ParseIP(host)
		if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
			return errors.New("must bind a loopback host")
		}
	}
	return nil
}
