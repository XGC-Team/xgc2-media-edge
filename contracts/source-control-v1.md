# Camera source control v1

This is the shared contract for native Gazebo camera sources, the ROS image RTP
adapter, and media-edge. Source processes own capture, encoder state, native
completion and configuration. Media-edge owns demand leases, WebRTC sessions,
immutable capture retention and the MediaMTX child. Neither owner launches the
other process. ROS user sensor data and an engine's same-process render path are
not RPC control paths.

Transport is XRPC `http.v1` on a canonical absolute Unix endpoint in an owned
0700 runtime directory, socket mode 0600. One bounded reusable SDK client per
source discovers a fresh process instance and fences every domain call with it.
No raw NDJSON socket, Base64 frame transfer or legacy route aliases remain.
Request identity and remaining deadline come from XRPC headers. A lost response
can follow an applied mutation; callers must preserve uncertain ownership and
observe the bound source before deciding whether to issue another operation.

| Method | Route | Meaning |
| --- | --- | --- |
| GET | `/v1/describe` | Service discovery only: `service_ref` and source roster |
| GET | `/v1/media/sources` | Metadata-only source roster |
| GET | `/v1/media/sources/{sourceId}/describe` | Source descriptor |
| GET | `/v1/media/sources/{sourceId}/status` | Desired/applied source state and native health |
| POST | `/v1/media/sources/{sourceId}/start` | Start native capture/encoding |
| POST | `/v1/media/sources/{sourceId}/stop` | Stop and confirm native completion |
| POST | `/v1/media/sources/{sourceId}/request-keyframe` | Native force-IDR only when advertised |
| POST | `/v1/media/sources/{sourceId}/capture` | One immutable capture transaction |
| GET | `/v1/media/sources/{sourceId}/config` | Current desired/applied configuration |
| PATCH | `/v1/media/sources/{sourceId}/config` | Revision-checked configuration |

`start`/`stop` take `{}`. Success returns
`{ok:true,source_id,state,active,completion:"applied",configuration_revision}`.
`active` is true for start and false for stop. HTTP 200, enqueue, or callback
entry does not constitute applied completion. Failure cannot return an applied
receipt. Idempotent reconciliation is a new, caller-owned operation, never
transport replay. Concurrent transitions are finite and domain-owned.

Status management fields use snake_case:
`{ok,source_id,state,desired_active,applied_active,configuration_revision,
 desired_revision,applied_revision,persisted_revision,last_error,native}`.
Native state is `idle`, `starting`, `active`, `stopping`, or `faulted`.
During a native transition desired/applied active values may differ. Caller
cancellation cannot release the domain transition owner or synthesize idle.
Only final active/idle postconditions return lifecycle applied receipts.
A native encoder daemon being alive does not prove a device exists or frames
arrive. Consumers keep that distinction from MediaMTX's RTP path availability,
inbound bytes, frame errors and decoded browser frames.

Descriptor and capture fields preserve their established camelCase spelling.
Descriptor includes `ok`, `protocolVersion:1`, `sourceId`, `codec:"H264"`,
`rtpPayloadType:96`, `rtpClockRate:90000`, `rtpHost`, `rtpPort`, `width`,
`height`, `fps`, `frameId`, `capabilities`, `keyframeRequestSupported`, and
`keyframePolicy`. Required capabilities are `start`, `stop`, `capture`,
`fresh-snapshot`. The configured loopback RTP destination and optional expected
geometry must match the descriptor before media-edge starts a listener.

A source with bounded GOP and no real force-IDR operation reports
`keyframeRequestSupported:false`, `keyframePolicy:"bounded-gop"`, omits the
`request-keyframe` capability, and rejects that operation with 501 unsupported.
Media-edge skips background keyframe requests for that source. A source with
native force-IDR advertises the capability and only reports its actual domain
completion. Fixed GOP never masquerades as a successful force-IDR request.

Configuration is ephemeral unless a separately implemented persistence owner
is declared. PATCH accepts
`{expected_revision:1,persist:false,config:{rtp_host,rtp_port,bitrate}}`.
GET/PATCH return `{ok,source_id,desired,applied,desired_revision,
 applied_revision,persisted_revision:null,persistence:"ephemeral",mutable_fields}`.
Unknown fields and invalid types fail. Stale revisions conflict. Persist true
is explicitly unsupported. A mutable bitrate or RTP destination is applied
only while idle; restart-required fields must return a declared conflict and
cannot be silently ignored. RTP destination changes require matching a new
media-edge listener configuration; the current edge does not reconfigure its
live MediaMTX ingress path.

Capture takes `{snapshotId,includeRgb,requireFresh,requestKeyframe}`. Optional
booleans default to true, false, false respectively. JPEG capture is independent
of H264 GOP; explicit force-IDR fails when unsupported. `requireFresh:true`
means a source frame accepted after capture request admission. The returned
JPEG, optional exact RGB8, dimensions, optical frame identity, `frameSequence`,
source timestamp and source clock all belong to that one immutable frame.

The response is native `multipart/mixed` with an ASCII boundary of at most 70
bytes. Ordered parts are `metadata` (`application/json`), `jpeg` (`image/jpeg`),
and optional `rgb` (`application/octet-stream`). Each has exactly one
`Content-Type` and `Content-Disposition: inline; name="..."`. A native writer may
add one canonical `Content-Length` equal to the actual part size. No MIME transfer
encoding, filename, duplicate header or extra part is allowed. Metadata is at
most 64 KiB; native MIME preamble/header phases are limited to 16 KiB plus the
native parser's 4096-byte read-ahead. JPEG is 2 bytes..32 MiB and RGB 0..128 MiB;
the announced lengths must equal actual part lengths. JPEG-only requests must
announce and send no RGB. These are transport bounds, not measured peak RSS.
Media-edge's effective JPEG+RGB capture ceiling defaults to 64 MiB and may be
lowered by its shared runtime policy before declared frame allocation.

Capture metadata includes `ok`, `sourceId`, `snapshotId`, `frameId`, `frameSequence`,
`timestampNanoseconds`, `timestampClockDomain`, `width`, `height`,
`pixelFormat:"rgb8"`, `jpegBytes`, `rgbBytes`, optional JPEG diagnostics,
`calibrationState`, optional `cameraMatrix`/`distortion`, and optional render
pose. Timestamp domains are `simulation`, `system_realtime`, `monotonic`,
`device`, or `unknown`; consumers never replace a missing source clock with wall
clock and call it source time. Zero simulation time is valid. Frame sequence
must be positive for new source implementations.

Calibration unavailable means no K/distortion is supplied; media-edge forwards
`calibrationState:"unavailable"` and still permits media capture. A real source
calibration has a finite 9-element K and 4..16 distortion coefficients. No
source or edge creates K from width merely to satisfy this API. Render pose, if
provided, is from the same capture and has a declared `poseFrameId`.

Media-edge admission defaults are at most 16 configured sources, 32 active or
negotiating WebRTC sessions, 16 simultaneous domain operations, one capture and
one coalesced background keyframe call per source. Source control pools have at
most two connections and four calls. A resource-exhausted capture/session fails
before adding another domain job. Caller cancellation does not prove the source
native work stopped. Sources and their SDK own that work until real completion.
Retention has two captures per source and a 128 MiB product-wide byte ceiling,
with oldest capture eviction. Retention is independent from an in-flight reply.

Media-edge's browser gateway uses SDK `ServeEdge` for finite connection/call
admission. Local capture uses the private `--rpc-socket`, service `media-edge`,
GET `/v1/describe` then an instance-bound POST
`/v1/media/sources/{sourceId}/capture`. The response follows the multipart
contract above; DELETE `/v1/media/snapshots/{snapshotId}` releases retention.
Public browser routes carry player assets, health and session signaling;
recording control remains restricted to loopback callers. Internal frame capture
never uses an unfenced browser signaling route. CLI `--rpc-socket` is a required
explicit deployment grant. It has no `/tmp` fallback. The installed
`/usr/lib/xgc2-media-edge/prepare-runtime` creates a dedicated runtime namespace
and owned 0700 control/MediaMTX children for a specified numeric UID/GID. It
rejects existing foreign/misconfigured directories and symlink ancestors.

The process composition root snapshots `XGC2_XRPC_` settings once and passes the
same SDK RuntimePolicy to public/private hosts and every source client. Explicit
settings for absent diagnostics, client-registry or gRPC owners fail startup.
Host call/connection/byte/time limits and source client pool limits execute at
their owners; product operation/capture admission also respects lower limits.
The private discovery response publishes the effective policy and its sources.

WHEP creation uses a fresh token and one native POST. A lost creation response
holds its session slot and source demand. Edge matches a complete finite native
inventory by unique token, source path and read state, then kicks the inventory's
API UUID, never the WHEP Location secret. [MediaMTX v1.20.0's API implementation](https://github.com/bluenviron/mediamtx/blob/v1.20.0/internal/api/api_webrtc.go)
and [native WebRTC owner](https://github.com/bluenviron/mediamtx/blob/v1.20.0/internal/servers/webrtc/server.go)
define that distinction. Inventory is at most 1024 sessions and 2 MiB; incomplete
pagination fails closed. Each reconcile pass admits at most four cleanup
attempts under a shared two-second deadline. Empty inventory before native
creation was ever observed leaves outcome unknown. A subsequent complete
absence after observation or a complete native failure response, confirmed
native deletion, or confirmed managed child exit releases ownership. The pinned
[native HTTP implementation](https://github.com/bluenviron/mediamtx/blob/v1.20.0/internal/servers/webrtc/http_server.go)
completes creation before returning its full failure response; inventory is
still checked rather than treating status alone as absence. Native cleanup
errors retain ownership for retry.

This contract is implemented and tested in isolated product fixtures. It does
not claim packaging, deployed source images, physical camera, native Gazebo
render acceptance, full 4K burst measurements or live station completion.
