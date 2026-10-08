package mediaedge

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/httpx"
	"io"
	"math"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const maximumControlHeaderBytes = 64 << 10
const maximumCameraPartHeaderBytes = 16 << 10
const maximumCameraJPEGBytes = 32 << 20
const maximumCameraRGBBytes = 128 << 20

const (
	sourceControlProtocolVersion = 1
	sourceRTPPayloadType         = 96
	sourceCodec                  = "H264"
)

var requiredSourceCapabilities = [...]string{
	"start",
	"stop",
	"capture",
	"fresh-snapshot",
}

type sourceControlRequest struct {
	Operation        string                      `json:"-"`
	SnapshotID       string                      `json:"snapshotId,omitempty"`
	IncludeRGB       *bool                       `json:"includeRgb,omitempty"`
	RequestKeyframe  *bool                       `json:"requestKeyframe,omitempty"`
	RequireFresh     *bool                       `json:"requireFresh,omitempty"`
	ExpectedRevision *uint64                     `json:"expected_revision,omitempty"`
	Persist          *bool                       `json:"persist,omitempty"`
	Config           *sourceRuntimeConfiguration `json:"config,omitempty"`
}

type sourceRuntimeConfiguration struct {
	RTPHost string `json:"rtp_host,omitempty"`
	RTPPort int    `json:"rtp_port,omitempty"`
	Bitrate int    `json:"bitrate,omitempty"`
}

type sourceControlResponse struct {
	ServiceRef                *xrpc.ServiceRef `json:"service_ref,omitempty"`
	OK                        bool             `json:"ok"`
	Error                     string           `json:"error,omitempty"`
	ProtocolVersion           int              `json:"protocolVersion,omitempty"`
	SourceID                  string           `json:"sourceId,omitempty"`
	ManagedSourceID           string           `json:"source_id,omitempty"`
	Codec                     string           `json:"codec,omitempty"`
	RTPPayloadType            int              `json:"rtpPayloadType,omitempty"`
	RTPClockRate              int              `json:"rtpClockRate,omitempty"`
	RTPHost                   string           `json:"rtpHost,omitempty"`
	RTPPort                   int              `json:"rtpPort,omitempty"`
	FPS                       float64          `json:"fps,omitempty"`
	Capabilities              []string         `json:"capabilities,omitempty"`
	KeyframeRequestSupported  bool             `json:"keyframeRequestSupported"`
	KeyframePolicy            string           `json:"keyframePolicy"`
	SnapshotJpegPolicy        string           `json:"snapshotJpegPolicy,omitempty"`
	SnapshotJpegBackend       string           `json:"snapshotJpegBackend,omitempty"`
	SnapshotJpegHardwareState string           `json:"snapshotJpegHardwareState,omitempty"`
	SnapshotID                string           `json:"snapshotId,omitempty"`
	FrameID                   string           `json:"frameId,omitempty"`
	// TimestampNanoseconds is in the source clock domain. A Gazebo source uses
	// simulation time, so calling it UnixNano would be materially incorrect.
	TimestampNanoseconds int64 `json:"timestampNanoseconds,omitempty"`
	// TimestampClockDomain uses the same vocabulary as xgc_camera_msgs/StreamInfo:
	// simulation, system_realtime, monotonic, device, or unknown.
	TimestampClockDomain  string                     `json:"timestampClockDomain,omitempty"`
	Width                 int                        `json:"width,omitempty"`
	Height                int                        `json:"height,omitempty"`
	PixelFormat           string                     `json:"pixelFormat,omitempty"`
	JPEGBytes             int                        `json:"jpegBytes,omitempty"`
	RGBBytes              int                        `json:"rgbBytes,omitempty"`
	JPEGBackend           string                     `json:"jpegBackend,omitempty"`
	JPEGReadback          string                     `json:"jpegReadback,omitempty"`
	JPEGFallbackReason    string                     `json:"jpegFallbackReason,omitempty"`
	JPEGReadbackMillis    float64                    `json:"jpegReadbackMilliseconds,omitempty"`
	JPEGEncodeMillis      float64                    `json:"jpegEncodeMilliseconds,omitempty"`
	CameraMatrix          []float64                  `json:"cameraMatrix,omitempty"`
	Distortion            []float64                  `json:"distortion,omitempty"`
	RenderPose            *SnapshotRenderPose        `json:"renderPose,omitempty"`
	PoseFrameID           string                     `json:"poseFrameId,omitempty"`
	Completion            string                     `json:"completion,omitempty"`
	Active                bool                       `json:"active,omitempty"`
	CalibrationState      string                     `json:"calibrationState,omitempty"`
	Sequence              uint64                     `json:"frameSequence,omitempty"`
	State                 string                     `json:"state,omitempty"`
	DesiredActive         bool                       `json:"desired_active"`
	AppliedActive         bool                       `json:"applied_active"`
	LastError             string                     `json:"last_error,omitempty"`
	ConfigurationRevision uint64                     `json:"configuration_revision"`
	Desired               sourceRuntimeConfiguration `json:"desired"`
	Applied               sourceRuntimeConfiguration `json:"applied"`
	DesiredRevision       uint64                     `json:"desired_revision"`
	AppliedRevision       uint64                     `json:"applied_revision"`
	PersistedRevision     *uint64                    `json:"persisted_revision"`
	Persistence           string                     `json:"persistence,omitempty"`
	MutableFields         []string                   `json:"mutable_fields,omitempty"`
}

func describeSource(ctx context.Context, config SourceConfig) (SourceConfig, error) {
	control := &cameraControl{socket: config.ControlSocket, instance: config.ControlInstanceID, sourceID: config.ID}
	defer control.close()
	return describeSourceWith(ctx, config, control)
}
func describeSourceWith(ctx context.Context, config SourceConfig, control *cameraControl) (SourceConfig, error) {
	response, _, _, err := control.call(ctx, sourceControlRequest{
		Operation: "describe",
	})
	if err != nil {
		return SourceConfig{}, fmt.Errorf("describe capture source: %w", err)
	}
	if response.ProtocolVersion != sourceControlProtocolVersion {
		return SourceConfig{}, fmt.Errorf(
			"capture source protocol version is %d, want %d",
			response.ProtocolVersion,
			sourceControlProtocolVersion,
		)
	}
	if response.SourceID != config.ID {
		return SourceConfig{}, fmt.Errorf(
			"capture source ID is %q, want %q",
			response.SourceID,
			config.ID,
		)
	}
	if response.Codec != sourceCodec ||
		response.RTPPayloadType != sourceRTPPayloadType ||
		response.RTPClockRate != h264RTPClockRate {
		return SourceConfig{}, fmt.Errorf(
			"capture source RTP contract is codec=%q payloadType=%d clockRate=%d, want %s/%d/%d",
			response.Codec,
			response.RTPPayloadType,
			response.RTPClockRate,
			sourceCodec,
			sourceRTPPayloadType,
			h264RTPClockRate,
		)
	}
	if err := validateSourceRTPDestination(
		config.RTPListenAddress,
		response.RTPHost,
		response.RTPPort,
	); err != nil {
		return SourceConfig{}, fmt.Errorf("capture source RTP destination: %w", err)
	}
	if response.Width < 16 || response.Height < 16 || int64(response.Width) > maximumCameraRGBBytes/3/int64(response.Height) ||
		response.FPS <= 0 || response.FPS > 240 || math.IsNaN(response.FPS) ||
		strings.TrimSpace(response.FrameID) == "" || len(response.FrameID) > 256 {
		return SourceConfig{}, errors.New("capture source describe metadata is invalid")
	}
	capabilities := make(map[string]struct{}, len(response.Capabilities))
	for _, capability := range response.Capabilities {
		capabilities[capability] = struct{}{}
	}
	for _, required := range requiredSourceCapabilities {
		if _, found := capabilities[required]; !found {
			return SourceConfig{}, fmt.Errorf(
				"capture source does not advertise required capability %q",
				required,
			)
		}
	}
	_, advertisesKeyframe := capabilities["request-keyframe"]
	if advertisesKeyframe != response.KeyframeRequestSupported || response.KeyframePolicy == "" {
		return SourceConfig{}, errors.New("capture source keyframe capability is inconsistent")
	}
	if config.hasExpectedMetadata() &&
		(response.Width != config.Width ||
			response.Height != config.Height ||
			!nominalFrameRatesMatch(response.FPS, config.FPS) ||
			response.FrameID != config.FrameID) {
		return SourceConfig{}, fmt.Errorf(
			"capture source metadata %dx%d@%g frameId=%q does not match expected %dx%d@%g frameId=%q",
			response.Width,
			response.Height,
			response.FPS,
			response.FrameID,
			config.Width,
			config.Height,
			config.FPS,
			config.FrameID,
		)
	}
	config.Width = response.Width
	config.Height = response.Height
	config.FPS = response.FPS
	config.FrameID = response.FrameID
	config.KeyframeRequestSupported = response.KeyframeRequestSupported
	return config, nil
}

func nominalFrameRatesMatch(actual float64, expected float64) bool {
	// Gazebo/SDF and camera-driver APIs may round the same nominal cadence
	// through float32 before returning it as a JSON float64 (for example,
	// 30 Hz becomes 30.0000003). Keep the metadata assertion strict enough to
	// reject a genuinely different mode while accepting representation noise.
	tolerance := math.Max(1e-6, math.Abs(expected)*1e-6)
	return math.Abs(actual-expected) <= tolerance
}

func validateSourceRTPDestination(expectedAddress string, describedHost string, describedPort int) error {
	expectedHost, expectedPortText, err := net.SplitHostPort(expectedAddress)
	if err != nil {
		return errors.New("configured listener must be a host:port value")
	}
	expectedPort, err := strconv.Atoi(expectedPortText)
	if err != nil || expectedPort < 1 || expectedPort > 65_535 {
		return errors.New("configured listener port is invalid")
	}
	describedHost = strings.TrimSpace(describedHost)
	if err := requireLoopbackHost(describedHost); err != nil {
		return fmt.Errorf("host %q %w", describedHost, err)
	}
	if describedPort != expectedPort {
		return fmt.Errorf(
			"port is %d, want configured listener port %d",
			describedPort,
			expectedPort,
		)
	}
	// Do not treat "localhost" as equivalent to a concrete address. Independent
	// resolution could select ::1 for the sender and 127.0.0.1 for the listener,
	// producing a valid-looking but permanently silent source.
	if expectedHost == "localhost" || describedHost == "localhost" {
		if expectedHost != describedHost {
			return fmt.Errorf("host is %q, want configured listener host %q", describedHost, expectedHost)
		}
		return nil
	}
	expectedIP := net.ParseIP(expectedHost)
	describedIP := net.ParseIP(describedHost)
	if expectedIP == nil || describedIP == nil || !expectedIP.Equal(describedIP) {
		return fmt.Errorf("host is %q, want configured listener host %q", describedHost, expectedHost)
	}
	return nil
}

// Snapshot is immutable data owned by the media edge. JPEG is for an operator
// display, while RGB is the exact same frame for local calibration algorithms.
// The two representations are created by one source-side snapshot transaction.
type Snapshot struct {
	ID                   string
	SourceID             string
	FrameID              string
	TimestampNanoseconds int64
	TimestampClockDomain string
	Width                int
	Height               int
	PixelFormat          string
	JPEG                 []byte
	RGB                  []byte
	JPEGBackend          string
	JPEGReadback         string
	JPEGFallbackReason   string
	JPEGReadbackMillis   float64
	JPEGEncodeMillis     float64
	CameraMatrix         []float64
	Distortion           []float64
	RenderPose           *SnapshotRenderPose
	PoseFrameID          string
	CalibrationState     string
	Sequence             uint64
	ExpiresAt            time.Time
}

// SnapshotCaptureRequest selects only source work needed by one local
// consumer. Nil requests full RGB without forcing a keyframe.
type SnapshotCaptureRequest struct {
	IncludeRGB      *bool `json:"includeRgb,omitempty"`
	RequestKeyframe *bool `json:"requestKeyframe,omitempty"`
	RequireFresh    *bool `json:"requireFresh,omitempty"`
}

func (request SnapshotCaptureRequest) includeRGB() bool {
	return request.IncludeRGB == nil || *request.IncludeRGB
}

func (request SnapshotCaptureRequest) requestKeyframe() bool {
	return request.RequestKeyframe != nil && *request.RequestKeyframe
}

// SnapshotRenderPose is the optional camera pose at the exact render captured
// by a source snapshot. Older sources omit both this value and PoseFrameID.
type SnapshotRenderPose struct {
	Position    SnapshotVector3    `json:"position"`
	Orientation SnapshotQuaternion `json:"orientation"`
}

type SnapshotVector3 struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
	Z float64 `json:"z"`
}

type SnapshotQuaternion struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
	Z float64 `json:"z"`
	W float64 `json:"w"`
}

func (snapshot Snapshot) metadata() snapshotMetadata {
	return snapshotMetadata{
		OK:         true,
		SnapshotID: snapshot.ID, SourceID: snapshot.SourceID, FrameID: snapshot.FrameID,
		TimestampNanoseconds: snapshot.TimestampNanoseconds, TimestampClockDomain: snapshot.TimestampClockDomain,
		Width: snapshot.Width, Height: snapshot.Height,
		PixelFormat: snapshot.PixelFormat, JPEGBytes: len(snapshot.JPEG), RGBBytes: len(snapshot.RGB), CameraMatrix: append([]float64(nil), snapshot.CameraMatrix...),
		JPEGBackend: snapshot.JPEGBackend, JPEGReadback: snapshot.JPEGReadback,
		JPEGFallbackReason: snapshot.JPEGFallbackReason,
		JPEGReadbackMillis: snapshot.JPEGReadbackMillis,
		JPEGEncodeMillis:   snapshot.JPEGEncodeMillis,
		Distortion:         append([]float64(nil), snapshot.Distortion...),
		RenderPose:         cloneSnapshotRenderPose(snapshot.RenderPose), PoseFrameID: snapshot.PoseFrameID,
		CalibrationState: snapshot.CalibrationState, Sequence: snapshot.Sequence,
	}
}

type snapshotMetadata struct {
	OK                   bool                `json:"ok"`
	SnapshotID           string              `json:"snapshotId"`
	SourceID             string              `json:"sourceId"`
	FrameID              string              `json:"frameId"`
	TimestampNanoseconds int64               `json:"timestampNanoseconds"`
	TimestampClockDomain string              `json:"timestampClockDomain"`
	Width                int                 `json:"width"`
	Height               int                 `json:"height"`
	PixelFormat          string              `json:"pixelFormat"`
	JPEGBytes            int                 `json:"jpegBytes"`
	RGBBytes             int                 `json:"rgbBytes"`
	JPEGBackend          string              `json:"jpegBackend,omitempty"`
	JPEGReadback         string              `json:"jpegReadback,omitempty"`
	JPEGFallbackReason   string              `json:"jpegFallbackReason,omitempty"`
	JPEGReadbackMillis   float64             `json:"jpegReadbackMilliseconds,omitempty"`
	JPEGEncodeMillis     float64             `json:"jpegEncodeMilliseconds,omitempty"`
	CameraMatrix         []float64           `json:"cameraMatrix,omitempty"`
	Distortion           []float64           `json:"distortion,omitempty"`
	RenderPose           *SnapshotRenderPose `json:"renderPose,omitempty"`
	PoseFrameID          string              `json:"poseFrameId,omitempty"`
	CalibrationState     string              `json:"calibrationState"`
	Sequence             uint64              `json:"frameSequence"`
}

func cloneSnapshotRenderPose(pose *SnapshotRenderPose) *SnapshotRenderPose {
	if pose == nil {
		return nil
	}
	copy := *pose
	return &copy
}

// cameraControl owns one reusable XRPC transport for its source lifecycle.
// Discovery is explicit and only /v1/describe is allowed without fencing.
type cameraControl struct {
	socket, instance, sourceID string
	mu                         sync.Mutex
	client                     *httpx.Client
	maxCaptureBytes            int64
	policy                     *xrpc.Policy
}

func (control *cameraControl) boundInstance() string {
	control.mu.Lock()
	defer control.mu.Unlock()
	return control.instance
}

func (control *cameraControl) close() {
	control.mu.Lock()
	defer control.mu.Unlock()
	if control.client != nil {
		control.client.Close()
	}
}
func (control *cameraControl) transport(ctx context.Context) (*httpx.Client, error) {
	control.mu.Lock()
	defer control.mu.Unlock()
	if control.client != nil {
		return control.client, nil
	}
	target, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	reference := xrpc.ServiceRef{TargetID: target, Service: "camera-source", APIVersion: "v1", InstanceID: control.instance, Profile: xrpc.HTTP, Endpoint: xrpc.Endpoint{Kind: "unix", Address: control.socket}}
	build := func(ref xrpc.ServiceRef) (*httpx.Client, error) {
		maxCaptureBytes := control.maxCaptureBytes
		if maxCaptureBytes == 0 {
			maxCaptureBytes = 64 << 20
		}
		options := httpx.Config{LocalTargetID: target, Service: ref, MaxConnections: 2, MaxInFlight: 4, MaxRequestBytes: 300 << 10, MaxResponseBytes: maxCaptureBytes + maximumControlHeaderBytes + 4*maximumCameraPartHeaderBytes}
		if control.policy != nil {
			var err error
			options, err = (httpx.Config{LocalTargetID: target, Service: ref, MaxInFlight: 4}).WithPolicy(control.policy)
			if err != nil {
				return nil, err
			}
			if limit, err := control.policy.Integer("HOST_MAX_IN_FLIGHT"); err == nil && limit < int64(options.MaxInFlight) {
				options.MaxInFlight = int(limit)
			}
		}
		return httpx.New(options)
	}
	client, err := build(reference)
	if err != nil {
		return nil, err
	}
	if reference.InstanceID == "" {
		id, err := newSnapshotID()
		if err != nil {
			client.Close()
			return nil, err
		}
		data, status, _, err := client.Do(ctx, http.MethodGet, "/v1/describe", id, "", nil)
		client.Close()
		if err != nil {
			return nil, err
		}
		if status != http.StatusOK {
			return nil, fmt.Errorf("camera discovery HTTP %d", status)
		}
		var description sourceControlResponse
		if err := json.Unmarshal(data, &description); err != nil {
			return nil, err
		}
		if description.ServiceRef == nil {
			return nil, errors.New("camera discovery omitted service_ref")
		}
		reference = *description.ServiceRef
		if err := reference.ValidateInternal(); err != nil {
			return nil, err
		}
		if reference.TargetID != target || reference.Endpoint.Kind != "unix" || reference.Endpoint.Address != control.socket || reference.Service != "camera-source" || reference.APIVersion != "v1" {
			return nil, errors.New("camera discovery reference does not match local endpoint")
		}
		client, err = build(reference)
		if err != nil {
			return nil, err
		}
	}
	control.client = client
	control.instance = reference.InstanceID
	return client, nil
}

// Used by boundary tests and one-shot tooling; the running media server retains
// cameraControl and therefore does not reconnect for every operation.
func callSourceControl(ctx context.Context, socket string, request sourceControlRequest) (sourceControlResponse, []byte, []byte, error) {
	control := &cameraControl{socket: socket, sourceID: "camera"}
	defer control.close()
	return control.call(ctx, request)
}
func (control *cameraControl) call(ctx context.Context, request sourceControlRequest) (sourceControlResponse, []byte, []byte, error) {
	timeout := sourceControlRequestTimeout
	if request.Operation == "capture" {
		timeout = sourceSnapshotRequestTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	client, err := control.transport(ctx)
	if err != nil {
		return sourceControlResponse{}, nil, nil, err
	}
	if request.Operation != "describe" && request.Operation != "status" && request.Operation != "config" && request.Operation != "configure" && request.Operation != "start" && request.Operation != "stop" && request.Operation != "request-keyframe" && request.Operation != "capture" {
		return sourceControlResponse{}, nil, nil, errors.New("unknown camera route")
	}
	body, err := json.Marshal(request)
	if err != nil {
		return sourceControlResponse{}, nil, nil, err
	}
	id, err := newSnapshotID()
	if err != nil {
		return sourceControlResponse{}, nil, nil, err
	}
	method, kind := http.MethodPost, "application/json"
	if request.Operation == "describe" || request.Operation == "status" || request.Operation == "config" {
		method, kind, body = http.MethodGet, "", nil
	}
	if !stableSourceID.MatchString(control.sourceID) {
		return sourceControlResponse{}, nil, nil, errors.New("camera source ID is required")
	}
	path := "/v1/media/sources/" + control.sourceID
	if request.Operation == "configure" {
		method = http.MethodPatch
		path += "/config"
	} else {
		path += "/" + request.Operation
	}
	reply, err := client.DoStream(ctx, method, path, id, kind, body)
	if err != nil {
		return sourceControlResponse{}, nil, nil, err
	}
	defer reply.Body.Close()
	if reply.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(io.LimitReader(reply.Body, maximumControlHeaderBytes))
		return sourceControlResponse{}, nil, nil, fmt.Errorf("camera HTTP %d: %s", reply.StatusCode, data)
	}
	if request.Operation == "capture" {
		maxBytes := control.maxCaptureBytes
		if maxBytes == 0 {
			maxBytes = 64 << 20
		}
		return readCameraMultipartWithLimit(reply.Header.Get("Content-Type"), reply.Body, request.IncludeRGB == nil || *request.IncludeRGB, maxBytes)
	}
	metadata, err := io.ReadAll(io.LimitReader(reply.Body, maximumControlHeaderBytes+1))
	if err != nil {
		return sourceControlResponse{}, nil, nil, err
	}
	if len(metadata) > maximumControlHeaderBytes {
		return sourceControlResponse{}, nil, nil, errors.New("camera metadata is too large")
	}
	var response sourceControlResponse
	if err = json.Unmarshal(metadata, &response); err != nil {
		return response, nil, nil, err
	}
	if !response.OK {
		return response, nil, nil, fmt.Errorf("camera rejected request: %s", response.Error)
	}
	if (request.Operation == "start" || request.Operation == "stop") &&
		(response.ManagedSourceID != control.sourceID || response.Completion != "applied" || response.Active != (request.Operation == "start") || response.ConfigurationRevision == 0 || (request.Operation == "start" && response.State != "active") || (request.Operation == "stop" && response.State != "idle")) {
		return response, nil, nil, errors.New("camera lifecycle omitted native applied completion")
	}
	return response, nil, nil, nil
}
func readCameraMultipart(contentType string, body io.Reader, includeRGB bool) (sourceControlResponse, []byte, []byte, error) {
	return readCameraMultipartWithLimit(contentType, body, includeRGB, maximumCameraJPEGBytes+maximumCameraRGBBytes)
}
func readCameraMultipartWithLimit(contentType string, body io.Reader, includeRGB bool, maxBytes int64) (sourceControlResponse, []byte, []byte, error) {
	fail := func(err error) (sourceControlResponse, []byte, []byte, error) {
		return sourceControlResponse{}, nil, nil, err
	}
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil || mediaType != "multipart/mixed" || params["boundary"] == "" || len(params["boundary"]) > 70 {
		return fail(errors.New("camera snapshot requires multipart/mixed"))
	}
	// Limit each native parser phase before allocation, including MIME preamble
	// and headers. mime/multipart buffers at most 4096 read-ahead bytes between
	// phases. We use NextRawPart and reject transfer encodings rather than
	// allowing a second decoded representation to evade byte accounting.
	bounded := &cameraPhaseReader{reader: body}
	reader := multipart.NewReader(bounded, params["boundary"])
	nextPart := func() (*multipart.Part, error) {
		bounded.remaining = maximumCameraPartHeaderBytes
		part, err := reader.NextRawPart()
		if err != nil {
			return nil, err
		}
		for name, values := range part.Header {
			if len(values) != 1 || (name != "Content-Type" && name != "Content-Disposition" && name != "Content-Length") {
				return nil, errors.New("camera MIME headers are invalid")
			}
		}
		if part.Header.Get("Content-Type") == "" || part.Header.Get("Content-Disposition") == "" {
			return nil, errors.New("camera MIME headers are incomplete")
		}
		return part, nil
	}
	part, err := nextPart()
	if err != nil {
		return fail(err)
	}
	if cameraPartName(part) != "metadata" || part.Header.Get("Content-Type") != "application/json" {
		return fail(errors.New("camera first part must be metadata JSON"))
	}
	bounded.remaining = maximumControlHeaderBytes + 4096
	metadata, err := io.ReadAll(io.LimitReader(part, maximumControlHeaderBytes+1))
	if err != nil {
		return fail(err)
	}
	if len(metadata) > maximumControlHeaderBytes {
		return fail(errors.New("camera metadata is too large"))
	}
	if err := checkCameraPartLength(part, len(metadata)); err != nil {
		return fail(err)
	}
	var response sourceControlResponse
	if err = json.Unmarshal(metadata, &response); err != nil {
		return fail(err)
	}
	if !response.OK || response.JPEGBytes < 2 || response.JPEGBytes > maximumCameraJPEGBytes || response.RGBBytes < 0 || response.RGBBytes > maximumCameraRGBBytes || (includeRGB && response.RGBBytes == 0) || (!includeRGB && response.RGBBytes != 0) {
		return fail(errors.New("capture source snapshot sizes are invalid"))
	}
	if int64(response.JPEGBytes)+int64(response.RGBBytes) > maxBytes {
		return fail(errors.New("camera snapshot exceeds capture byte budget"))
	}
	readPart := func(name, kind string, size int) ([]byte, error) {
		part, err := nextPart()
		if err != nil {
			return nil, err
		}
		if cameraPartName(part) != name || part.Header.Get("Content-Type") != kind {
			return nil, fmt.Errorf("expected camera %s part", name)
		}
		if err := checkCameraPartLength(part, size); err != nil {
			return nil, err
		}
		bounded.remaining = int64(size) + 4096
		data := make([]byte, size)
		if _, err = io.ReadFull(part, data); err != nil {
			return nil, err
		}
		var extra [1]byte
		if n, err := part.Read(extra[:]); n != 0 || err != io.EOF {
			return nil, fmt.Errorf("camera %s size mismatch", name)
		}
		return data, nil
	}
	jpeg, err := readPart("jpeg", "image/jpeg", response.JPEGBytes)
	if err != nil {
		return fail(err)
	}
	var rgb []byte
	if response.RGBBytes > 0 {
		rgb, err = readPart("rgb", "application/octet-stream", response.RGBBytes)
		if err != nil {
			return fail(err)
		}
	}
	if _, err = nextPart(); err != io.EOF {
		return fail(errors.New("unexpected extra camera part"))
	}
	return response, jpeg, rgb, nil
}

func checkCameraPartLength(part *multipart.Part, size int) error {
	value := part.Header.Get("Content-Length")
	if value != "" && value != strconv.Itoa(size) {
		return errors.New("camera MIME Content-Length mismatch")
	}
	return nil
}

type cameraPhaseReader struct {
	reader    io.Reader
	remaining int64
}

func (reader *cameraPhaseReader) Read(buffer []byte) (int, error) {
	if reader.remaining <= 0 {
		return 0, errors.New("camera MIME phase exceeds byte limit")
	}
	if int64(len(buffer)) > reader.remaining {
		buffer = buffer[:reader.remaining]
	}
	n, err := reader.reader.Read(buffer)
	reader.remaining -= int64(n)
	return n, err
}

func newSnapshotID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("create snapshot ID: %w", err)
	}
	return hex.EncodeToString(value), nil
}

func cameraPartName(part *multipart.Part) string {
	disposition, params, err := mime.ParseMediaType(part.Header.Get("Content-Disposition"))
	if err != nil || disposition != "inline" || len(params) != 1 {
		return ""
	}
	return params["name"]
}
