// xgc-media-edge is the target-resident XGC video data plane. It intentionally
// has no Core or Agent URL: browsers signal directly to its HTTP endpoint, and
// ICE/TURN carries live RTP directly between each browser and this process.
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
	"github.com/pion/webrtc/v4"
)

var version = "dev"

func main() {
	var (
		controlAddress          = flag.String("control-address", "127.0.0.1:18090", "HTTP listen address; explicitly bind a target interface for remote browsers")
		sourcesConfig           = flag.String("sources-config", "", "JSON file containing one or more local media sources; mutually exclusive with legacy single-source flags")
		sourceID                = flag.String("source-id", "", "stable media source ID")
		rtpAddress              = flag.String("rtp-listen-address", "", "loopback H264/RTP ingress address")
		controlSocket           = flag.String("source-control-socket", "", "absolute Unix socket owned by the capture source")
		width                   = flag.Int("width", 0, "optional expected source pixel width; provide all four metadata assertions or none")
		height                  = flag.Int("height", 0, "optional expected source pixel height; provide all four metadata assertions or none")
		fps                     = flag.Float64("fps", 0, "optional expected source frame rate; provide all four metadata assertions or none")
		frameID                 = flag.String("frame-id", "", "optional expected source optical frame ID; provide all four metadata assertions or none")
		allowedOrigins          multiString
		publicIPs               multiString
		iceURLs                 multiString
		iceUsername             = flag.String("ice-username", "", "optional shared TURN username")
		iceCredential           = flag.String("ice-credential", "", "optional shared TURN credential")
		grace                   = flag.Duration("session-grace", 10*time.Second, "idle source stop delay")
		snapshotTTL             = flag.Duration("snapshot-ttl", 2*time.Minute, "immutable snapshot retention")
		recordingRoot           = flag.String("recording-root", "", "absolute local recording root; empty disables recording")
		recordingFFmpeg         = flag.String("recording-ffmpeg", "", "FFmpeg executable used only for H264 stream-copy Matroska muxing")
		recordingMaxBitrate     = flag.Uint64("recording-max-bitrate", 0, "configured source peak bitrate in bits/s; required with --recording-root")
		recordingQueue          = flag.Int("recording-queue-packets", 0, "bounded recorder RTP queue; default 8192")
		recordingSegment        = flag.Duration("recording-segment-duration", 0, "target segment duration; cuts at the next IDR, default 5m")
		recordingMaxDuration    = flag.Duration("recording-max-duration", 0, "maximum accepted recording duration, default 24h")
		recordingFinalize       = flag.Duration("recording-finalize-timeout", 0, "per-segment FFmpeg finalize timeout, default 15s")
		recordingKeyframe       = flag.Duration("recording-keyframe-timeout", 0, "maximum wait for SPS/PPS+IDR, default 8s")
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

	sources, err := resolveSources(*sourcesConfig, legacySourceFlags{
		ID: *sourceID, RTPListenAddress: *rtpAddress, ControlSocket: *controlSocket,
		Width: *width, Height: *height, FPS: *fps, FrameID: *frameID,
	})
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
			FFmpegPath:              *recordingFFmpeg,
			MaxBitrateBitsPerSecond: *recordingMaxBitrate,
			QueuePackets:            *recordingQueue,
			SegmentDuration:         *recordingSegment,
			MaxDuration:             *recordingMaxDuration,
			FinalizeTimeout:         *recordingFinalize,
			KeyframeTimeout:         *recordingKeyframe,
			MinimumFreeBytes:        *recordingMinimumFree,
			CapacitySafetyFactor:    *recordingCapacityFactor,
		},
	}
	if len(iceURLs) > 0 {
		config.ICEServers = []webrtc.ICEServer{{
			URLs: append([]string(nil), iceURLs...), Username: strings.TrimSpace(*iceUsername), Credential: *iceCredential,
		}}
	}
	server, err := mediaedge.New(config)
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
