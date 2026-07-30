# XGC2 Media Edge

`xgc2-media-edge` is the target-resident XGC2 video data plane. Each process
instance accepts one configured, co-located H264/RTP camera source, exposes a
small direct-browser signaling API, and fans that encoded source out to WebRTC
viewers without decoding, transcoding, or starting another encoder per viewer.

The product is intentionally independent from ROS, Gazebo, USB camera drivers,
XGC2 Core, and XGC2 Agent. Those systems produce media or coordinate processes;
this repository owns only the media-edge executable and its protocols. Core and
Agent are not part of a browser connection.

## Runtime boundary

```text
Gazebo or USB capture source
  |-- H264/RTP over loopback UDP
  `-- describe/lifecycle/keyframe/snapshot over a Unix socket
                      |
                      v
               xgc-media-edge
                 |           |-- WebRTC/SRTP to browser viewers
                 |           `-- optional H264 stream-copy recording
                 `-- HTTP signaling directly from the browser
```

The HTTP listener defaults to `127.0.0.1:18090`. A deployment may explicitly
bind it to a target interface or `0.0.0.0` so a browser can reach it. RTP ingress
is different: it is always loopback-only and cannot be relaxed. The process has
no Core URL, performs no discovery, and never dials a ground station. A
browser-originated offer determines the ICE path used for SRTP.

The package installs no systemd unit. XGC2's process catalog remains the single
owner of process definitions, readiness probes, resource declarations, and
restart policy when it is used, but supervision is optional and never enters
the connection path.

## Browser surface

The remotely reachable HTTP surface is deliberately small:

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/` | Embedded, dependency-free WebRTC player |
| `GET` | `/assets/player.css` | Embedded player style |
| `GET` | `/assets/player.js` | Embedded player signaling logic |
| `GET` | `/healthz` | Metadata-only health |
| `POST` | `/api/v1/sources/{sourceId}/sessions` | Non-trickle SDP offer to answer |
| `DELETE` | `/api/v1/sessions/{sessionId}` | Deterministic session teardown |

HTTP snapshot creation and retrieval retain their existing paths but reject
every non-loopback client. Recording control is also loopback-only and is
documented below. Live video is never served as HTTP pixels: there is no MJPEG,
HLS, JPEG polling, discovery, SSE, or WebSocket API.

The embedded page receives only the configured source ID from the server. Its
plain browser code creates a recv-only `RTCPeerConnection`, waits for local ICE
gathering, attaches the remote track to `<video>`, and deletes the session when
the page exits.

Same-origin use of the embedded player needs no CORS configuration. To embed
the same direct protocol in another WebUI, repeat `--allowed-origin` with each
exact HTTP(S) origin:

```bash
--allowed-origin https://station.example:8443
```

Origins are normalized without a trailing slash and must not contain
credentials, a path, query, or fragment. CORS is not authentication; this
initial direct mode is intended for a trusted LAN or VPN. The service does not
terminate TLS. Consequently, an HTTPS WebUI must use an HTTPS termination point
for Edge as well, while the standalone page can be opened directly over HTTP in
the trusted network.

## Source contract

Each process instance is configured with one source that supplies:

- a stable source ID;
- a loopback H264/RTP endpoint using payload type 96;
- an absolute Unix control socket;

On startup Edge sends:

```json
{"operation":"describe"}
```

The source must answer one newline-delimited JSON object:

```json
{
  "ok": true,
  "protocolVersion": 1,
  "sourceId": "usb_cam",
  "codec": "H264",
  "rtpPayloadType": 96,
  "rtpClockRate": 90000,
  "rtpHost": "127.0.0.1",
  "rtpPort": 5004,
  "width": 1920,
  "height": 1080,
  "fps": 30,
  "frameId": "usb_cam_optical",
  "capabilities": ["set-active", "request-keyframe", "snapshot"]
}
```

This description is the runtime authority for the source's loopback RTP
destination, width, height, frame rate, and optical frame ID. The RTP endpoint
must use a fixed port from 1 through 65535 and match Edge's configured listener,
which turns a mistyped camera/Edge port into a startup error instead of a
permanently black stream. The corresponding media CLI values are optional
deployment assertions: omit all four to learn them from the source, or provide
all four and require an exact match. A source ID, codec, RTP contract, endpoint,
metadata, or capability mismatch prevents Edge from opening any listener.

The same newline-delimited JSON Unix protocol supports:

- `describe`;
- `set-active`;
- `request-keyframe`;
- `snapshot`.

A snapshot is one immutable transaction containing display JPEG bytes, exact
RGB8 bytes, camera intrinsics, distortion coefficients, frame ID, and a
source-clock timestamp. New sources also identify that timestamp's clock
domain using the `xgc_camera_msgs/StreamInfo` vocabulary. A source may return
the fields below; Edge passes them through without requiring them from older
protocol-v1 sources:

```json
{
  "timestampClockDomain": "simulation",
  "renderPose": {
    "position": {"x": 1.0, "y": 2.0, "z": 3.0},
    "orientation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0}
  },
  "poseFrameId": "world"
}
```

`timestampClockDomain` is one of `simulation`, `system_realtime`, `monotonic`,
`device`, or `unknown`. If an older source omits both domain and timestamp,
Edge explicitly labels its local fallback `system_realtime`. `renderPose` is
the camera pose for the exact snapshot render, expressed in `poseFrameId`; it
is intended for evidence packages and calibration. Live pixels never use HTTP
polling.

## Optional H264 recording

Recording is disabled unless `--recording-root` is set. Enabling it also
requires an explicit source peak bitrate for conservative capacity admission:

```bash
--recording-root /var/lib/xgc2/media-recordings \
--recording-max-bitrate 36000000
```

FFmpeg is an optional runtime dependency used solely as a Matroska muxer. When
recording is enabled, Edge resolves `--recording-ffmpeg` (default `ffmpeg`) at
startup and fails with a clear error if it is unavailable. The invoked pipeline
accepts Annex-B H264 and uses `-c:v copy`; it never decodes, re-encodes, changes
quality, or allocates another NVENC session. Preview-only deployments do not
need FFmpeg.

One receive loop performs RTP continuity rewriting once, then sends that same
encoded stream to both branches:

- Pion's shared WebRTC RTP track;
- a non-blocking, bounded recorder queue and independent writer.

A slow disk or muxer therefore cannot block preview. Queue overflow, RTP packet
loss, malformed H264 packetization, and source clock restarts are recorded as
discontinuities. The writer closes the preceding valid segment, discards
dependent frames, requests a keyframe, and resumes only from a complete
SPS/PPS+IDR access unit. Segment duration is a target: a cut occurs at the first
IDR at or after the configured duration, never between dependent frames. Only
one recording may be active per source; any number of viewers may join or leave
without changing it. A recording is itself a source consumer, so zero viewers
does not stop capture.

Recording control follows the snapshot security boundary and rejects every
non-loopback client:

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/api/v1/sources/{sourceId}/recordings` | Start a bounded recording |
| `GET` | `/api/v1/recordings` | List active and finalized recordings |
| `GET` | `/api/v1/recordings/{recordingId}` | Read current/final status |
| `DELETE` | `/api/v1/recordings/{recordingId}` | Stop and finalize |

Start requests require an integer duration:

```bash
curl --fail-with-body \
  -H 'Content-Type: application/json' \
  -d '{"durationSeconds":3600}' \
  http://127.0.0.1:18090/api/v1/sources/usb_cam/recordings
```

Before creating output, Edge checks filesystem space using:

```text
peak bitrate / 8 * requested duration * capacity safety factor
  + minimum retained free bytes
```

The defaults are a 1.20 safety factor and 1 GiB retained free space. Admission
uses the configured peak bitrate, not an observed average. The maximum accepted
duration defaults to 24 hours.

Each recording has a generated ID and stays beneath the canonical configured
root:

```text
RECORDING_ROOT/
  RECORDING_ID/
    manifest.json
    segments/
      segment-000001.mkv
      segment-000001.frames.jsonl
      segment-000002.mkv
      segment-000002.frames.jsonl
```

FFmpeg writes `.mkv.part`; Edge fsyncs and atomically renames it only after a
successful finalize. The per-segment JSONL frame index follows the same rule
and records the rewritten RTP timestamp, RTP sequence range, keyframe flag,
Annex-B byte count, segment-relative 90 kHz PTS, and Edge ingress UTC for every
complete access unit. Edge ingress UTC is a receive-time diagnostic, not a
claim about camera exposure or Gazebo simulation time. Offline synchronization
must correlate the RTP timeline to a separately defined source clock. Matroska
playback uses the configured nominal frame rate; the JSONL RTP timeline is
authoritative when source frames were skipped or an exact external time join is
required.

`manifest.json` is atomically replaced at segment boundaries and final stop. It
contains source and codec identity, requested and actual times, queue high-water
and overflow counts, RTP loss/discontinuities, access-unit/keyframe totals,
bytes, finalized segments, and any failure reason. A restart preserves
previously finalized segments and marks a nonterminal manifest failed; orphan
`.part` files are never listed as finalized media.

Relevant tuning flags are:

- `--recording-queue-packets` (default `8192`);
- `--recording-segment-duration` (default `5m`);
- `--recording-max-duration` (default `24h`);
- `--recording-keyframe-timeout` (default `8s`);
- `--recording-finalize-timeout` (default `15s`);
- `--recording-minimum-free-bytes` (default `1073741824`);
- `--recording-capacity-safety-factor` (default `1.20`).

## Build and test from source

Go 1.26.2 or newer is required:

```bash
go test ./...
go test -race ./...
./.xgc2/scripts/build.sh
```

The development build is written to `.ci/bin/xgc-media-edge`. This source build
is the intended integration-test input while the interfaces are under active
development; Debian packages are the production artifact.

## Run

Example for a co-located source:

```bash
./.ci/bin/xgc-media-edge \
  --control-address 0.0.0.0:18090 \
  --allowed-origin http://192.168.1.20:3000 \
  --source-id usb_cam \
  --rtp-listen-address 127.0.0.1:5004 \
  --source-control-socket /tmp/xgc2/media/usb_cam.sock \
  --recording-root /var/lib/xgc2/media-recordings \
  --recording-max-bitrate 13500000
```

`--public-ip` and `--ice-server` are optional. They are deployment inputs, not
ground-station discovery. Repeat either flag when multiple values are needed.
The HTTP port carries signaling only; ICE selects the UDP path for SRTP, so the
target firewall must also permit the deployment's selected ICE path.

Readiness remains available locally even when the listener is remotely bound:

```bash
curl --fail http://127.0.0.1:18090/healthz
```

From another machine, opening `http://TARGET_IP:18090/` starts a direct session
with the configured source.

## Debian package

Build the native `xgc2-media-edge` package:

```bash
PACKAGE_DISTRIBUTION=noble ./.xgc2/scripts/build_deb.sh
```

The package installs `/usr/bin/xgc-media-edge`. CI builds and runtime-smoke
tests Focal, Jammy, and Noble packages on native amd64 and arm64 runners.

## License

This repository uses the BSD 3-Clause License, matching the permissive license
style already used by the XGC2 common product repositories. See [LICENSE](LICENSE).
