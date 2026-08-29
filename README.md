# XGC2 Media Edge

`xgc2-media-edge` is the target-resident XGC2 video data plane. Each process
instance accepts one or more configured, co-located H264/RTP sources, exposes a
small direct-browser signaling API, and fans each encoded source out to WebRTC
viewers without decoding, transcoding, or starting another encoder per viewer.

The product is intentionally independent from ROS, Gazebo, and camera drivers,
XGC2 Core, and XGC2 Agent. Those systems produce media or coordinate processes;
this repository owns only the media-edge executable and its protocols. Core and
Agent are not part of a browser connection.

The Gazebo camera product currently implements this RTP/Unix-socket source
contract directly. The ROS USB camera driver publishes encoded camera data and
timing into ROS, but does not itself provide the RTP/control adapter; a physical
camera deployment must supply a separate conforming adapter before pairing that
source with Media Edge.

## Runtime boundary

```text
Conforming H264 capture source
  |-- H264/RTP over loopback UDP
  `-- describe/lifecycle/keyframe/snapshot over a Unix socket
                      |
                      v
        xgc-media-edge control layer
          |-- source leases, snapshots, recording intent, manifests
          `-- WHEP compatibility proxy
                       |
                       v
               pinned MediaMTX
                 |-- WebRTC/SRTP over fixed 18189/UDP
                 `-- optional native stream-copy fMP4 recording
```

The HTTP listener defaults to `127.0.0.1:18090`. A deployment may explicitly
bind it to a target interface or `0.0.0.0` so a browser can reach it. RTP ingress
is different: it is always loopback-only and cannot be relaxed. The process has
no Core URL, performs no discovery, and never dials a ground station. A
browser-originated offer is proxied to MediaMTX WHEP. MediaMTX owns RTP
parsing, fanout, ICE and SRTP; the fixed direct ICE listener is
`0.0.0.0:18189/udp` unless explicitly configured otherwise.

The package installs no systemd unit. XGC2's process catalog remains the single
owner of process definitions, readiness probes, resource declarations, and
restart policy when it is used, but supervision is optional and never enters
the connection path.

### UDP receive-buffer contract

The generated MediaMTX configuration requests `udpReadBufferSize: 8388608`
(8 MiB). A 4K H264 frame is fragmented across many loopback RTP packets; the
ordinary Linux 208 KiB receive queue loses FU-A fragments when motion increases
the encoded frame burst, even when the source camera itself remains at 30 Hz.
Deployments must therefore set `net.core.rmem_max >= 8388608` before Edge opens
its RTP socket. XGC2 local-fleet owns that host-network setup explicitly and
fails its runtime status check when the contract is absent. Edge still does not
auto-tune or silently retry with a different value.

Size the queue from a bounded encoder, never from a static-scene FPS reading.
For one RTP source, a conservative userspace request is

```text
Bsocket >= S × (VBVbits / 8 + Rmax × Tsched / 8) × (1 + Hrtp-ip-udp / Prtp)
```

where `VBVbits` is the encoder VBV buffer (a conservative access-unit burst
bound), `Rmax` is the configured maximum bitrate, `Tsched` is the longest
receiver scheduling pause admitted by the deployment, `Prtp` is RTP payload
bytes per datagram, `Hrtp-ip-udp` is the corresponding header bytes, and `S`
is the safety factor. Without explicit `maxrate` and `VBVbits`, nominal average
bitrate provides no finite burst bound and this calculation is invalid.

The local-fleet 4K30 contract uses `Rmax = VBVbits = 25,000,000`,
`Tsched = 0.1 s`, `Prtp = 1200`, approximately 40 header bytes, and `S = 2`:
the result is about 7.1 MB, rounded up to 8 MiB. Linux may report roughly twice
the requested value in `ss -m` because of kernel accounting; do not multiply
the engineering requirement by that reporting convention. Each source owns a
separate socket queue, and the kernel consumes the capacity on demand rather
than reserving the full maximum for every idle source.

Every Debian package matrix entry proves this contract with the real bundled
MediaMTX binary. The automated probe extracts that binary, runs it as numeric
UID/GID 65532 with all Linux capabilities dropped, no external network, a
read-only root filesystem, and `no-new-privileges`, then waits for the actual
MediaMTX API to report the configured RTP path. No mock process or readiness
response is used. That deliberately isolated binary probe sets only its
temporary MediaMTX config to the OS-default queue because it cannot raise the
host `rmem_max`; source config tests independently keep the product default at
8 MiB, and each deployment still proves the host prerequisite before startup.

## Browser surface

The remotely reachable HTTP surface is deliberately small:

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/`, `/?source={sourceId}`, `/sources/{sourceId}` | Embedded shared-React WebRTC player |
| `GET` | `/assets/player.css` | Embedded shared XGC2 UI and player style |
| `GET` | `/assets/player.js` | Embedded React player and signaling logic |
| `GET` | `/healthz` | Metadata-only health |
| `POST` | `/api/v1/sources/{sourceId}/sessions` | Non-trickle SDP offer to answer |
| `DELETE` | `/api/v1/sessions/{sessionId}` | Deterministic session teardown |

The source-scoped session endpoints are the stable XGC product contract. They
are a thin WHEP proxy and do not implement a second WebRTC stack.

HTTP snapshot creation and retrieval retain their existing paths but reject
every non-loopback client. Recording control is also loopback-only and is
documented below. Live video is never served as HTTP pixels: there is no MJPEG,
HLS, JPEG polling, discovery, SSE, or WebSocket API.

The embedded page receives only its selected source ID from the server. Its
React entry consumes the immutable `@xgc2/ui-react` release for the single-title
topbar, theme control, panel, status text, action control, tokens, responsive
layout, and global scrollbar contract. Product code creates a recv-only
`RTCPeerConnection`, waits for local ICE gathering, attaches the remote track to
`<video>`, and deletes the session when the page exits. The default `/` player selects the first configured source;
`?source=` and `/sources/` select another source without creating another HTTP
service.

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

Each configured source supplies:

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
  "sourceId": "camera",
  "codec": "H264",
  "rtpPayloadType": 96,
  "rtpClockRate": 90000,
  "rtpHost": "127.0.0.1",
  "rtpPort": 5004,
  "width": 1920,
  "height": 1080,
  "fps": 30,
  "frameId": "camera_optical",
  "capabilities": ["set-active", "request-keyframe", "snapshot", "fresh-snapshot"]
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

A snapshot is one immutable transaction containing display JPEG bytes, camera
intrinsics, distortion coefficients, frame ID, and a source-clock timestamp.
The legacy/default request also carries exact RGB8 bytes; a detector may send
`{"includeRgb":false,"requestKeyframe":false,"requireFresh":true}` to receive
only the first source frame completed after its request, without coupling the
transaction to the H264 GOP. Sources may additionally report the actual JPEG
backend/readback path, fallback reason, and bounded timing diagnostics; Edge
retains these fields with the immutable snapshot metadata.

New sources also identify the timestamp's clock domain using the
`xgc_camera_msgs/StreamInfo` vocabulary. A source may return the fields below;
Edge passes them through without requiring them from older protocol-v1
sources:

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

MediaMTX writes native fragmented MP4 from the same encoded H264 stream used by
WHEP viewers. XGC does not decode, transcode, rewrite RTP, run FFmpeg, or own a
second recorder queue. Only one recording may be active per source; viewers can
join or leave independently. A recording retains the source lease, so zero
viewers does not stop capture.

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
  http://127.0.0.1:18090/api/v1/sources/camera/recordings
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
      camera-segment-2026-08-09_12-00-00.mp4
      camera-segment-2026-08-09_12-05-00.mp4
```

The actual files are MediaMTX-native `.mp4` segments. XGC scans only finalized
regular files beneath the canonical recording directory and records their
relative path, byte size and lifecycle times in `manifest.json`.

`manifest.json` is atomically replaced at segment boundaries and final stop. It
contains source and codec identity, requested and actual times, bytes, finalized
segments, and any failure reason. Compatibility counters that belonged to the
retired private muxer remain zero. A restart preserves previously finalized
segments and marks a nonterminal manifest failed.

Relevant tuning flags are:

- `--recording-segment-duration` (default `5m`);
- `--recording-max-duration` (default `24h`);
- `--recording-finalize-timeout` (default `15s`);
- `--recording-minimum-free-bytes` (default `1073741824`);
- `--recording-capacity-safety-factor` (default `1.20`).

## Build and test from source

Node.js 22 and Go 1.26.2 or newer are required:

```bash
npm --prefix web ci
npm --prefix web run build
go test ./...
go test -race ./...
./.xgc2/scripts/build.sh
```

The frontend build writes deterministic `player.js` and `player.css` assets
beside the Go HTML template. Commit those generated files with their React
source. CI rebuilds them from the immutable shared package and rejects drift.

The development build is written to `.ci/bin/xgc-media-edge`. This source build
is the intended integration-test input while the interfaces are under active
development; Debian packages are the production artifact.

### Real 4K RTP burst acceptance

The non-privileged startup probe is deliberately not presented as a throughput
test. A target profile is accepted for 4K only with a real H264 encoder and RTP
packetizer feeding the packaged MediaMTX process under the same container,
kernel, CPU, and network limits used in deployment. A physical source adapter
is preferred. A GStreamer `videotestsrc` is acceptable only when its pixels pass
through the real 3840x2160 encoder and `rtph264pay`; generated UDP bytes, mocked
MediaMTX APIs, and injected counters are not evidence.

The repeatable profile is 3840x2160 at 30 frames/s, payload type 96, 90 kHz RTP,
MTU 1200, the deployment's declared peak bitrate, and one IDR per second for at
least ten minutes. Source-side instrumentation must retain sent RTP packet and
byte counts plus the largest one-, ten-, and one-hundred-millisecond bursts.
During the same run one real WHEP receiver must decode continuously and native
MediaMTX recording must be enabled. Acceptance requires all of the following:

- MediaMTX reports an H264 3840x2160 track, increasing inbound bytes, and zero
  `inboundFramesInError` from start through final IDR;
- receiver WebRTC statistics show decoded frames and keyframes advancing with
  no RTP packet loss or decode freeze across every measured burst;
- `ffprobe` identifies the finalized recording as H264 3840x2160 with the
  expected duration, and a full decode reports no corrupt frame;
- logs contain no UDP read, RTP sequence, reader-too-slow, or frame decode
  error, and rerunning the profile three times produces the same result;
- the receipt records package digest, MediaMTX version, kernel/container limits,
  encoder settings, sender burst measurements, API snapshots, receiver stats,
  and recording digest.

A failure rejects that deployment profile. It does not cause Edge to select a
larger buffer dynamically or add a privileged execution path.

## Run

Every source roster, including a single-source deployment, is one strict JSON
document. A ready-to-copy two-source document is checked in at
`examples/two-sources.json`:

```json
{
  "sources": [
    {
      "id": "front",
      "rtpListenAddress": "127.0.0.1:5004",
      "controlSocket": "/tmp/xgc2/media/front.sock"
    },
    {
      "id": "world",
      "rtpListenAddress": "127.0.0.1:5006",
      "controlSocket": "/tmp/xgc2/media/world.sock",
      "width": 3840,
      "height": 2160,
      "fps": 30,
      "frameId": "world_camera_optical"
    }
  ]
}
```

```bash
./.ci/bin/xgc-media-edge \
  --control-address 0.0.0.0:18090 \
  --allowed-origin http://192.168.1.20:3000 \
  --recording-root /var/lib/xgc2/media-recordings \
  --recording-max-bitrate 13500000 \
  --sources-config /run/xgc2/media/sources.json
```

Unknown JSON fields, an empty source list, duplicate IDs, non-loopback RTP
listeners, reused UDP ports, or a mismatch with a source's authoritative
`describe` response fail startup. Every source keeps its own control socket,
viewer count, active state, RTP counters, snapshots, and recordings while
sharing the Edge HTTP/WebRTC listener.

`--public-ip` and `--ice-server` are optional. They are deployment inputs, not
ground-station discovery. Repeat either flag when multiple values are needed.
The HTTP port carries signaling only. The target firewall and container mapping
must permit fixed `18189/udp`; additional STUN/TURN inputs do not reintroduce a
private fanout implementation.

Readiness remains available locally even when the listener is remotely bound:

```bash
curl --fail http://127.0.0.1:18090/healthz
```

From another machine, opening `http://TARGET_IP:18090/` starts a direct session
with the first source. Open `http://TARGET_IP:18090/sources/world` for a named
source; product panels should call the existing source-scoped session API.

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
