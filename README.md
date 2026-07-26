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
                 |           |
                 |           `-- WebRTC/SRTP to browser viewers
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
every non-loopback client. Live video is never served as HTTP pixels: there is
no MJPEG, HLS, JPEG polling, discovery, SSE, or WebSocket API.

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
source-clock timestamp. Live pixels never use HTTP polling.

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
  --source-control-socket /tmp/xgc2/media/usb_cam.sock
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
