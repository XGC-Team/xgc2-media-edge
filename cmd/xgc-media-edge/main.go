// xgc-media-edge is the target-resident XGC video product boundary. It owns
// Experiment/source lifecycle and delegates the standard RTP/WebRTC/WHEP media
// plane to a pinned MediaMTX child. It intentionally has no Core or Agent URL.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/lxk36/xgc2-media-edge/internal/mediaedge"
)

var version = "dev"

func main() {
	var (
		controlAddress          = flag.String("control-address", "127.0.0.1:18090", "HTTP listen address; explicitly bind a target interface for remote browsers")
		mediaMTXExecutable      = flag.String("mediamtx-executable", "/usr/lib/xgc2-media-edge/mediamtx", "absolute path to the pinned MediaMTX binary")
		mediaMTXRuntimeDir      = flag.String("mediamtx-runtime-dir", "/run/xgc2/media-edge", "absolute private runtime directory for generated MediaMTX configuration")
		mediaMTXAPIAddress      = flag.String("mediamtx-api-address", "127.0.0.1:19997", "loopback-only MediaMTX control API address")
		mediaMTXWHEPAddress     = flag.String("mediamtx-whep-address", "127.0.0.1:18889", "loopback-only MediaMTX WHEP HTTP address; XGC proxies browser signaling")
		mediaMTXICEUDPAddress   = flag.String("webrtc-ice-udp-address", "0.0.0.0:18189", "MediaMTX fixed WebRTC ICE UDP listener")
		mediaMTXICETCPAddress   = flag.String("webrtc-ice-tcp-address", "", "optional MediaMTX fixed WebRTC ICE TCP listener")
		mediaMTXInterfaceIPs    = flag.Bool("webrtc-interface-ips", true, "advertise target interface IPs as ICE candidates")
		sourcesConfig           = flag.String("sources-config", "", "required JSON file containing one or more local media sources")
		allowedOrigins          multiString
		publicIPs               multiString
		iceURLs                 multiString
		iceUsername             = flag.String("ice-username", "", "optional shared TURN username")
		iceCredential           = flag.String("ice-credential", "", "optional shared TURN credential")
		grace                   = flag.Duration("session-grace", 10*time.Second, "idle source stop delay")
		snapshotTTL             = flag.Duration("snapshot-ttl", 2*time.Minute, "immutable snapshot retention")
		recordingRoot           = flag.String("recording-root", "", "absolute local recording root; empty disables recording")
		recordingMaxBitrate     = flag.Uint64("recording-max-bitrate", 0, "configured source peak bitrate in bits/s; required with --recording-root")
		recordingSegment        = flag.Duration("recording-segment-duration", 0, "target segment duration; cuts at the next IDR, default 5m")
		recordingMaxDuration    = flag.Duration("recording-max-duration", 0, "maximum accepted recording duration, default 24h")
		recordingFinalize       = flag.Duration("recording-finalize-timeout", 0, "MediaMTX segment finalization timeout, default 15s")
		recordingMinimumFree    = flag.Uint64("recording-minimum-free-bytes", 0, "filesystem space retained after capacity admission, default 1 GiB")
		recordingCapacityFactor = flag.Float64("recording-capacity-safety-factor", 0, "peak-bitrate capacity multiplier, default 1.20")
		printVersion            = flag.Bool("version", false, "print version and exit")
	)
	flag.Var(&allowedOrigins, "allowed-origin", "exact cross-origin WebUI origin, for example https://station.example:8443; repeat as needed")
	flag.Var(&publicIPs, "public-ip", "public ICE address; repeat for multiple addresses")
	flag.Var(&iceURLs, "ice-server", "STUN/TURN URL; repeat for multiple URLs")
	flag.Parse()
	if *printVersion {
		fmt.Println(version)
		return
	}

	sources, err := resolveSources(*sourcesConfig)
	if err != nil {
		log.Fatalf("invalid XGC media-edge source configuration: %v", err)
	}
	config := mediaedge.Config{
		ControlAddress:     *controlAddress,
		AllowedOrigins:     append([]string(nil), allowedOrigins...),
		Sources:            sources,
		PublicIPs:          append([]string(nil), publicIPs...),
		SessionGracePeriod: *grace,
		SnapshotTTL:        *snapshotTTL,
		Recording: mediaedge.RecordingConfig{
			Root:                    *recordingRoot,
			MaxBitrateBitsPerSecond: *recordingMaxBitrate,
			SegmentDuration:         *recordingSegment,
			MaxDuration:             *recordingMaxDuration,
			FinalizeTimeout:         *recordingFinalize,
			MinimumFreeBytes:        *recordingMinimumFree,
			CapacitySafetyFactor:    *recordingCapacityFactor,
		},
	}
	if len(iceURLs) > 0 {
		config.ICEServers = []mediaedge.ICEServerConfig{{
			URLs: append([]string(nil), iceURLs...), Username: strings.TrimSpace(*iceUsername), Credential: *iceCredential,
		}}
	}
	server, err := mediaedge.NewMediaMTX(config, mediaedge.MediaMTXSettings{
		Executable: *mediaMTXExecutable, RuntimeDir: *mediaMTXRuntimeDir,
		APIAddress: *mediaMTXAPIAddress, WHEPAddress: *mediaMTXWHEPAddress,
		ICEUDPAddress: *mediaMTXICEUDPAddress, ICETCPAddress: *mediaMTXICETCPAddress,
		IPsFromInterfaces: *mediaMTXInterfaceIPs,
	})
	if err != nil {
		log.Fatalf("invalid XGC media-edge configuration: %v", err)
	}
	if err := server.Start(); err != nil {
		log.Fatalf("start XGC media edge: %v", err)
	}
	log.Printf("xgc-media-edge ready on %s for sources %s", server.ControlAddress(), sourceIDs(sources))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	<-ctx.Done()
	if err := server.Close(); err != nil {
		fmt.Fprintln(os.Stderr, "stop XGC media edge:", err)
	}
}

type multiString []string

func (values *multiString) String() string { return strings.Join(*values, ",") }

func (values *multiString) Set(value string) error {
	for _, candidate := range strings.Split(value, ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate != "" {
			*values = append(*values, candidate)
		}
	}
	return nil
}
