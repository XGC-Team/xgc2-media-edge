# XGC2 Media Edge

`xgc2-media-edge` is the target-resident XGC2 video data plane. Each process
instance accepts one configured, co-located H264/RTP camera source, exposes a
loopback-only control API, and fans that encoded source out to WebRTC viewers
without decoding, transcoding, or starting another encoder per viewer.

The product is intentionally independent from ROS, Gazebo, USB camera drivers,
XGC2 Core, and XGC2 Agent. Those systems produce media or coordinate processes;
this repository owns only the media-edge executable and its protocols.

## Runtime boundary

```text
Gazebo or USB capture source
  |-- H264/RTP over loopback UDP
  `-- lifecycle/keyframe/snapshot over a Unix socket
                      |
                      v
               xgc-media-edge
                      |
                      `-- WebRTC/SRTP to browser viewers
```

The HTTP control listener and RTP ingress must bind to loopback. The process has
no Core URL and never discovers or dials a ground station. A browser-originated
WebRTC session determines the ICE path used for SRTP.

The package installs no systemd unit. XGC2's process catalog remains the single
owner of process definitions, readiness probes, resource declarations, and
restart policy.

## Source contract

Each process instance is configured with one source that supplies:

- a stable source ID;
- a loopback H264/RTP endpoint using payload type 96;
- an absolute Unix control socket;
- width, height, frame rate, and optical frame ID.

The newline-delimited JSON Unix protocol currently supports:

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
  --control-address 127.0.0.1:18090 \
  --source-id usb_cam \
  --rtp-listen-address 127.0.0.1:5004 \
  --source-control-socket /tmp/xgc2/media/usb_cam.sock \
  --width 1920 \
  --height 1080 \
  --fps 30 \
  --frame-id usb_cam_optical
```

`--public-ip` and `--ice-server` are optional. They are deployment inputs, not
ground-station discovery. Repeat either flag when multiple values are needed.

Readiness is available from the target itself:

```bash
curl --fail http://127.0.0.1:18090/healthz
```

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
