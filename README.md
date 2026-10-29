# XGC2 Media Edge

`xgc2-media-edge` is the target-resident XGC2 video data plane. Each process
instance accepts one or more configured, co-located H264/RTP sources, exposes a
small direct-browser signaling API, and fans each encoded source out to WebRTC
viewers without decoding, transcoding, or starting another encoder per viewer.

The product is intentionally independent from ROS, Gazebo, and camera drivers,
XGC2 Core, and XGC2 Agent. Those systems produce media or coordinate processes;
this repository owns only the media-edge executable and its protocols. Core and
Agent are not part of a browser connection.

The Gazebo camera plugin and the XGC2 camera driver node implement this
RTP/Unix-socket source contract directly. A device that has no such process of
its own, such as a V4L2 camera on a robot or a ROS image topic, is run by the
media edge itself as a [pipeline source](#pipeline-sources).

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
no Core URL and never dials a ground station. It discovers only configured local source incarnations. A
browser-originated offer is proxied to MediaMTX WHEP. MediaMTX owns RTP
parsing, fanout, ICE and SRTP; the fixed direct ICE listener is
`0.0.0.0:18189/udp` unless explicitly configured otherwise.

The package installs no systemd unit. XGC2's process catalog remains the single
owner of process definitions, readiness probes, resource declarations, and
restart policy when it is used, but supervision is optional and never enters
the connection path.

### UDP receive-buffer contract

The generated MediaMTX configuration requests `udpReadBufferSize: 4194304`
(4 MiB). The host must permit this explicit request. Edge does not change host
sysctls or silently retry with a smaller queue. Qualify each source with
high-entropy motion, socket drops and end-to-end presentation delay; a successful
startup alone does not establish sufficient queue capacity.

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

Linux may report roughly twice the requested value because of kernel accounting;
do not multiply the engineering requirement by that reporting convention. Each
source owns a separate socket queue, consumed on demand.

Every Debian package matrix entry probes startup with the real bundled
MediaMTX binary. The automated probe extracts that binary, runs it as numeric
UID/GID 65532 with all Linux capabilities dropped, no external network, a
read-only root filesystem, and `no-new-privileges`, then waits for the actual
MediaMTX API to report the configured RTP path. No mock process or readiness
response is used. That deliberately isolated binary probe sets only its
temporary MediaMTX config to the OS-default queue because it cannot raise the
host `rmem_max`; source config tests independently keep the product default at
4 MiB, and each deployment still proves the host prerequisite before startup.

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

Capture uses the private instance-bound XRPC endpoint described below.
Recording control remains loopback-only and is documented below. Live video is never served as HTTP pixels: there is no MJPEG,
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

The implemented shared contract is [source-control-v1](contracts/source-control-v1.md).
Each source owns one XRPC `http.v1` Unix socket beneath an owned 0700 runtime
directory, with a fresh process incarnation and socket mode 0600. Edge discovers
GET `/v1/describe`, binds its returned `facts.service_ref`, and validates GET
`/v1/media/sources/{sourceId}/describe` when a viewer, capture, or source-reference
request first needs that peer. A missing source never blocks startup or edge
readiness. Describe facts and health report each source's control state.
XRPC matches discovery by path only, so GET `/v1/describe?wait_ready_ms=N`
remains an unbound discovery request.

One shared inotify watcher observes socket removal and recreation, including
late runtime directories. Each requested peer has one reconnect flight, using
held readiness discovery and full jitter from 250 ms to a 30 s cap. Ready
discovery returns immediately, so the connected worker waits for socket or
call-failure events instead of continuously rearming it. Failed viewers and
captures receive unavailable responses; the player retries with the same
bounded backoff and recovers without a page reload. An owned lease has a
monotonic desired revision and a confirmed applied revision for its peer
instance. A replacement receives the latest start or stop once; a lost receipt
is resolved from native status before another write to the same revision.

The source descriptor is authoritative for loopback H264/RTP payload 96 at
90 kHz, fixed destination port, geometry, cadence, optical frame and capabilities.
Optional deployment geometry assertions must all match it. Internal control is
instance-bound `start`, `stop`, `request-keyframe`, `capture`, `status` and
revision-checked ephemeral `config`; only native completion returns `applied`.
There are no legacy NDJSON, set-active or snapshot control aliases.

JPEG capture is independent of H264 GOP. Sources that cannot force a real IDR
advertise bounded GOP and explicitly reject force-IDR. Edge only requests a
keyframe when a source advertises actual support. A capture retains one frame's
JPEG, optional exact RGB8, source clock/time, optical frame and frame sequence.
Native multipart streams these buffers without Base64 or a whole-frame join.
Missing camera intrinsics are reported as `calibrationState: unavailable`;
Edge never invents K from image dimensions or substitutes wall clock for missing
source time.

Local capture clients discover the edge's explicitly granted `--rpc-socket`
and bind its returned incarnation. Instance-bound GET
`/v1/media/sources/{sourceId}/ref` returns the source-owned ServiceRef observed
during lazy discovery. Calibration clients can bind that reference with the
official SDK and call the source's typed configuration API directly, retaining
its calibration and publication receipts. After a restart the endpoint returns
the freshly discovered reference; unavailable peers return 503. Clients that
retained an earlier reference remain fenced to that earlier instance.

Capture clients use the same bound edge incarnation (service `media-edge`) and
issue instance-bound POST `/v1/media/sources/{sourceId}/capture` with
`{includeRgb:false,requestKeyframe:false,requireFresh:true}`. The result is the
same native multipart contract. DELETE `/v1/media/snapshots/{snapshotId}`
releases retained capture data. The former unfenced TCP snapshot API is removed.
Browser signaling remains on its direct HTTP listener.

The runnable [SDK client example](examples/capture_client/main.go) discovers and
binds the edge incarnation, streams one JPEG capture to disk with the unchanged
same-frame JSON metadata, closes its response, then explicitly deletes retention:

```bash
go run ./examples/capture_client \
  --rpc-socket /run/xgc2/media-edge/control/control.sock \
  --source front --output front.jpg
```

It requires no former TCP capture entry. The example's native MIME header phases,
metadata and JPEG length are bounded, and capture POST is never retried.

Resource admission defaults are 16 configured sources, 32 established or
negotiating sessions, 16 domain calls, one capture and one coalesced keyframe call
per source. A capture is at most 64 MiB JPEG+RGB (`--max-capture-bytes`), with
32 MiB JPEG and 128 MiB RGB absolute ceilings. Retention holds at most two
captures per source and 128 MiB product-wide (`--max-retained-snapshot-bytes`),
evicting the oldest captures first. The limit applies before declared payload
allocation. MIME metadata is at most 64 KiB; each native parser header/preamble
phase is bounded to 16 KiB plus 4096 bytes of native read-ahead. These finite
policy limits are not measured peak RSS or a 4K throughput claim.

Source health separates edge demand/uncertainty, the bound source's native
applied state and the MediaMTX RTP path. A failed native stop retains the source
client and ownership for another explicit Close; it never reports that source
inactive merely because the reply was lost. Source restart rejects stale calls;
a product owner must establish a fresh edge rather than rediscovering and
replaying mutations implicitly.

WHEP creation keeps its source lease and bounded session slot when the creation
reply is lost. Native inventory and the MediaMTX API session UUID permit cleanup
without replaying POST. Inventory is capped at 1024 complete entries and 2 MiB;
each reconcile pass has four cleanup attempts and a shared two-second deadline.
Unknown absence retains ownership until creation/deletion is observed or the
managed MediaMTX child has actually exited.

Browser/private hosts and source client pools use plain transport limits from
`Config`. Defaults are 32 host connections, 16 admitted host calls (the domain
operation limit), two connections and at most four admitted calls per source,
16 KiB headers, 300 KiB requests, and the capture budget plus 128 KiB for response
metadata and part headers. Call, header, idle and shutdown timeouts are 15, 3, 30
and 5 seconds. Lower host call and response limits also constrain domain operation
and capture admission. Private discovery publishes the domain limits.

## Pipeline sources

A source normally is a process of its own that implements the camera-source
contract. Where there is none, the edge runs the encoder: a source with
`"kind": "pipeline"` names a template and its parameters. The edge starts the
pipeline when the first viewer, capture or recording needs the source and stops it
when the last confirmed demand leaves. `--session-grace` remains the cleanup
delay for an ambiguous failed activation. The pipeline sends H264/RTP to the
source's `rtpListenAddress` like any other source.

```json
{
  "sources": [
    {
      "id": "fs150_cam", "kind": "pipeline", "rtpListenAddress": "127.0.0.1:5004",
      "width": 640, "height": 480, "fps": 30, "frameId": "fs150_cam_optical_frame",
      "pipeline": {
        "template": "x264-v4l2src-raw",
        "parameters": {"device": "/dev/video8", "format": "NV12", "bitrateKbps": 2500}
      }
    },
    {
      "id": "color", "kind": "pipeline", "rtpListenAddress": "127.0.0.1:5006",
      "width": 640, "height": 480, "fps": 30, "frameId": "d435_optical_frame",
      "pipeline": {
        "template": "nvv4l2-fdsrc",
        "parameters": {"format": "bgr", "bitrate": 2500000},
        "producer": ["/opt/ros/melodic/lib/xgc2_camera_driver/xgc2_ros_image_pipe", "--topic", "/d435/image_raw", "--format", "bgr8"]
      }
    },
    {
      "id": "simulated_camera", "kind": "pipeline", "rtpListenAddress": "127.0.0.1:5008",
      "width": 640, "height": 480, "fps": 30, "frameId": "sim_camera_optical_frame",
      "pipeline": {
        "template": "x264-ros-image",
        "parameters": {"topic": "/sim/camera/image_raw", "rosDistro": "noetic", "bitrateKbps": 2500}
      }
    }
  ]
}
```

There is no descriptor to ask, so `width`, `height`, `fps` (an integer) and
`frameId` are required and authoritative, `rtpListenAddress` must name a loopback
IP address, and `controlSocket` is not allowed. `--pipeline-templates` (default
`/usr/share/xgc2-media-edge/pipelines`) holds the templates and
`--pipeline-runtime-dir` (default `/run/xgc2/media-edge/pipelines`, created by
`prepare-runtime`) the scratch directory `<dir>/<sourceId>` of each source, owned
by the edge user with mode 0700 and emptied on every start.

### Templates

A template is a JSON file with the argument vector of the pipeline. What runs is
that vector, never a shell line. `${name}` placeholders in its arguments are
filled with the parameters of the source, and a value that does not match its
parameter's pattern is refused, so a parameter cannot add a pipeline element or
an argument. The edge supplies `width`, `height`, `fps`, `rtpHost`, `rtpPort`,
`payloadType` and, for a template with a snapshot branch, `snapshotDir`.

| Template | Input | Encoder | Parameters (default) |
| --- | --- | --- | --- |
| `x264-v4l2src-raw` | V4L2 device, raw video | software x264 | `device`, `format` (NV12), `bitrateKbps` (2500) |
| `x264-v4l2src-mjpeg` | V4L2 device, motion JPEG | software x264 | `device`, `bitrateKbps` |
| `x264-fdsrc` | raw frames on standard input | software x264 | `format` (bgr), `bitrateKbps` |
| `x264-ros-image` | ROS image topic via a template-owned producer | software x264 | `topic`, `rosDistro` (noetic; melodic or noetic), `bitrateKbps` (2500) |
| `nvv4l2-v4l2src-raw`, `-mjpeg`, `nvv4l2-fdsrc` | as above | Jetson `nvv4l2h264enc` | as above, with `bitrate` in bit/s (2500000) |
| `mpp-v4l2src-raw`, `-mjpeg`, `mpp-fdsrc` | as above | Rockchip `mpph264enc` | as above, with `bitrate` in bit/s (2500000) |

Simulated cameras use `x264-ros-image`, as in the example above and
`examples/pipeline-sources.json`. The template runs
`/opt/ros/${rosDistro}/lib/xgc2_camera_driver/xgc2_ros_image_pipe` with the
absolute `topic`, `--format bgr8`, the edge-supplied `--width` and `--height`, and
`--max-fps` set to the source's `fps`. The driver converts supported incoming
encodings to bgr8, so the encoder is fixed to `format=bgr`. `topic` must match
`/[A-Za-z0-9_/]{1,200}` and `rosDistro` must be `melodic` or `noetic`.
The source must omit `producer`; supplying one is refused with the template's
name. The simulator owns the source; no station camera node is started for it.

Every template sets one IDR per second (the GOP is `fps` frames), repeats SPS and
PPS with each of them, and packetizes with payload type 96 and MTU 1200. A
template that reads standard input needs a `producer` argument vector in the
source unless it declares its own `producer` vector. Keeping `"input": "stdin"`
for `x264-ros-image` uses the existing owned producer and encoder lifecycle
without adding an input kind. A template-owned producer uses the same validated
placeholders as the encoder and refuses a source-supplied producer; only an
explicit template `producer` declaration supplies one. The camera driver's
`xgc2_ros_image_pipe` writes the frames of a ROS image topic for the `-fdsrc` templates; its
`--format` `rgb8`, `bgr8` and `mono8` are the `format` values `rgb`, `bgr` and
`gray8`. A template with `"input": "device"` takes no producer. A template names
itself, declares its parameters (`default`, `pattern`, `description`) and says
whether it has a snapshot branch:

```json
{
  "name": "x264-fdsrc",
  "input": "stdin",
  "snapshot": true,
  "parameters": {"format": {"default": "bgr", "pattern": "rgb|bgr|gray8"}},
  "command": ["gst-launch-1.0", "-q", "fdsrc", "fd=0", "!", "rawvideoparse", "format=${format}", "..."]
}
```

The software templates need the GStreamer tools and the `-good` and `-ugly`
plugin sets (recommended by the package). The Jetson and Rockchip templates need
the vendor plugins of the board. `x264-ros-image` also needs the camera driver's
pipe installed for the selected ROS distribution and a reachable ROS master.
This repository tests the software templates with the real GStreamer; the
vendor templates use the properties documented for
`nvv4l2h264enc` and `mpph264enc` and have not run on that hardware.

### Lifecycle

A pipeline source presents the camera-source contract to the rest of the edge:

- `start` spawns the pipeline and returns when it has stayed up for half a
  second; a pipeline that exits sooner fails the start with its last output.
  `stop` interrupts it (SIGINT, so that GStreamer releases the device and the
  hardware encoder) and kills what remains after 1.5 seconds. Starting a running
  pipeline changes nothing.
- The pipeline and its producer end together: whichever exits first ends the
  other, and the source is `faulted` with the cause until the edge starts it
  again, which its recovery does when a watched source stops sending. Both are
  killed when the edge itself dies, so a crash cannot leave a camera held.
- `request-keyframe` restarts the pipeline, whose stream begins with an IDR
  (`keyframePolicy: "restart"`). A request within five seconds of the start is
  coalesced, since the stream began with one and the template repeats one every
  second. Viewer admission does not make this destructive request: existing
  viewers keep their source, and shipped templates provide periodic IDRs.
- `capture` needs the snapshot branch: one JPEG per second written to the
  scratch directory, two files kept. It returns the newest complete file as JPEG
  and, unless `includeRgb` is false, as decoded RGB8. With `requireFresh` it waits
  for a file completed after the request. A template without the branch lacks the
  `capture` capability and capture answers 501. There are no camera intrinsics
  (`calibrationState: "unavailable"`), and the source clock is the system clock.
- The pipeline's standard error goes to the edge's log, line by line, prefixed
  with the source and the process.
- There is no runtime configuration: `config` shows the RTP destination and
  `configure` is refused. A pipeline source has no service reference, so
  `/v1/media/sources/{sourceId}/ref` answers 404.

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

Recording control rejects every
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
XGC_MEDIA_SOURCE_FIXTURE=1 go test -race ./internal/mediaedge -run TestRealPythonSourceControlAndEdgeCapture -v
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
      "controlSocket": "/run/xgc2/media/front/source.sock"
    },
    {
      "id": "world",
      "rtpListenAddress": "127.0.0.1:5006",
      "controlSocket": "/run/xgc2/media/world/source.sock",
      "width": 3840,
      "height": 2160,
      "fps": 30,
      "frameId": "world_camera_optical"
    }
  ]
}
```

```bash
sudo /usr/lib/xgc2-media-edge/prepare-runtime \
  --runtime-root /run/xgc2/media-edge --uid "$(id -u)" --gid "$(id -g)"

./.ci/bin/xgc-media-edge \
  --control-address 0.0.0.0:18090 \
  --target-id "$(hostname)" \
  --rpc-socket /run/xgc2/media-edge/control/control.sock \
  --mediamtx-runtime-dir /run/xgc2/media-edge/mediamtx \
  --pipeline-runtime-dir /run/xgc2/media-edge/pipelines \
  --allowed-origin http://192.168.1.20:3000 \
  --recording-root /var/lib/xgc2/media-recordings \
  --recording-max-bitrate 13500000 \
  --sources-config /run/xgc2/media/sources.json
```

Runtime preparation is an explicit deployment action, required before an
unprivileged owner can use `/run`. Existing foreign/misconfigured directories and
symlink ancestors fail; startup has no temporary-directory fallback. Source
adapter runtime grants are owned separately by their deployments.

Unknown JSON fields, an empty source list, duplicate IDs, non-loopback RTP
listeners, or reused UDP ports fail startup. A mismatch with a source's
authoritative `describe` response fails that source's lease instead. Every source keeps its own control socket,
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

Reconnect regression checks run without camera hardware:

```bash
go test -p 2 ./...
XGC_MEDIA_LONG_TESTS=1 go test -p 2 ./internal/mediaedge -run TestCameraPeerDown60Seconds -count=1
node web/scripts/test-reconnect.mjs --long
```

The source CI cell runs both 60-second outage tests. They measure CPU time,
retry attempts, log lines, file descriptors and threads, then verify recovery.
Install the existing web dependencies with `npm --prefix web ci` before the
player check.

Build the native `xgc2-media-edge` package:

```bash
PACKAGE_DISTRIBUTION=noble ./.xgc2/scripts/build_deb.sh
```

The package installs `/usr/bin/xgc-media-edge`. CI builds and runtime-smoke
tests Focal, Jammy, and Noble packages on native amd64 and arm64 runners.

## License

This repository uses the BSD 3-Clause License, matching the permissive license
style already used by the XGC2 common product repositories. See [LICENSE](LICENSE).

`xgc-media-edge capture` reads one JSON object from stdin: the actual `service` reference, `sourceId`, existing absolute `artifactRoot`, `imageFormat` (`jpeg` or `png`), `includeRawRGB`, and `label`. It writes an atomically committed same-frame image/metadata bundle and returns its paths as JSON. The service reference remains instance-bound; the command releases native snapshot retention after reading the response. Use an ordinary finite process node; Core does not need camera-specific nodes or source addresses.
