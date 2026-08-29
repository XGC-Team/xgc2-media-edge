package mediaedge

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"strings"
	"time"
)

const maximumControlHeaderBytes = 64 << 10

const (
	sourceControlProtocolVersion = 1
	sourceRTPPayloadType         = 96
	sourceCodec                  = "H264"
)

var requiredSourceCapabilities = [...]string{
	"set-active",
	"request-keyframe",
	"snapshot",
	"fresh-snapshot",
}

type sourceControlRequest struct {
	Operation       string `json:"operation"`
	SnapshotID      string `json:"snapshotId,omitempty"`
	Active          *bool  `json:"active,omitempty"`
	IncludeRGB      *bool  `json:"includeRgb,omitempty"`
	RequestKeyframe *bool  `json:"requestKeyframe,omitempty"`
	RequireFresh    *bool  `json:"requireFresh,omitempty"`
}

type sourceControlResponse struct {
	OK              bool     `json:"ok"`
	Error           string   `json:"error,omitempty"`
	ProtocolVersion int      `json:"protocolVersion,omitempty"`
	SourceID        string   `json:"sourceId,omitempty"`
	Codec           string   `json:"codec,omitempty"`
	RTPPayloadType  int      `json:"rtpPayloadType,omitempty"`
	RTPClockRate    int      `json:"rtpClockRate,omitempty"`
	RTPHost         string   `json:"rtpHost,omitempty"`
	RTPPort         int      `json:"rtpPort,omitempty"`
	FPS             float64  `json:"fps,omitempty"`
	Capabilities    []string `json:"capabilities,omitempty"`
	SnapshotID      string   `json:"snapshotId,omitempty"`
	FrameID         string   `json:"frameId,omitempty"`
	// TimestampNanoseconds is in the source clock domain. A Gazebo source uses
	// simulation time, so calling it UnixNano would be materially incorrect.
	TimestampNanoseconds int64 `json:"timestampNanoseconds,omitempty"`
	// TimestampClockDomain uses the same vocabulary as xgc_camera_msgs/StreamInfo:
	// simulation, system_realtime, monotonic, device, or unknown.
	TimestampClockDomain string              `json:"timestampClockDomain,omitempty"`
	Width                int                 `json:"width,omitempty"`
	Height               int                 `json:"height,omitempty"`
	PixelFormat          string              `json:"pixelFormat,omitempty"`
	JPEGBytes            int                 `json:"jpegBytes,omitempty"`
	RGBBytes             int                 `json:"rgbBytes,omitempty"`
	CameraMatrix         []float64           `json:"cameraMatrix,omitempty"`
	Distortion           []float64           `json:"distortion,omitempty"`
	RenderPose           *SnapshotRenderPose `json:"renderPose,omitempty"`
	PoseFrameID          string              `json:"poseFrameId,omitempty"`
}

func describeSource(ctx context.Context, config SourceConfig) (SourceConfig, error) {
	response, _, _, err := callSourceControl(ctx, config.ControlSocket, sourceControlRequest{
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
	if response.Width < 16 || response.Height < 16 ||
		response.FPS <= 0 || response.FPS > 240 ||
		strings.TrimSpace(response.FrameID) == "" {
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
	CameraMatrix         []float64
	Distortion           []float64
	RenderPose           *SnapshotRenderPose
	PoseFrameID          string
	ExpiresAt            time.Time
}

// SnapshotCaptureRequest selects only source work needed by one local
// consumer. Nil retains the original full RGB + keyframe behavior.
type SnapshotCaptureRequest struct {
	IncludeRGB      *bool `json:"includeRgb,omitempty"`
	RequestKeyframe *bool `json:"requestKeyframe,omitempty"`
	RequireFresh    *bool `json:"requireFresh,omitempty"`
}

func (request SnapshotCaptureRequest) includeRGB() bool {
	return request.IncludeRGB == nil || *request.IncludeRGB
}

func (request SnapshotCaptureRequest) requestKeyframe() bool {
	return request.RequestKeyframe == nil || *request.RequestKeyframe
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
		SnapshotID: snapshot.ID, SourceID: snapshot.SourceID, FrameID: snapshot.FrameID,
		TimestampNanoseconds: snapshot.TimestampNanoseconds, TimestampClockDomain: snapshot.TimestampClockDomain,
		Width: snapshot.Width, Height: snapshot.Height,
		PixelFormat: snapshot.PixelFormat, JPEGBytes: len(snapshot.JPEG), CameraMatrix: append([]float64(nil), snapshot.CameraMatrix...),
		Distortion: append([]float64(nil), snapshot.Distortion...),
		RenderPose: cloneSnapshotRenderPose(snapshot.RenderPose), PoseFrameID: snapshot.PoseFrameID,
	}
}

type snapshotMetadata struct {
	SnapshotID           string              `json:"snapshotId"`
	SourceID             string              `json:"sourceId"`
	FrameID              string              `json:"frameId"`
	TimestampNanoseconds int64               `json:"timestampNanoseconds"`
	TimestampClockDomain string              `json:"timestampClockDomain"`
	Width                int                 `json:"width"`
	Height               int                 `json:"height"`
	PixelFormat          string              `json:"pixelFormat"`
	JPEGBytes            int                 `json:"jpegBytes"`
	CameraMatrix         []float64           `json:"cameraMatrix"`
	Distortion           []float64           `json:"distortion"`
	RenderPose           *SnapshotRenderPose `json:"renderPose,omitempty"`
	PoseFrameID          string              `json:"poseFrameId,omitempty"`
}

func cloneSnapshotRenderPose(pose *SnapshotRenderPose) *SnapshotRenderPose {
	if pose == nil {
		return nil
	}
	copy := *pose
	return &copy
}

func callSourceControl(
	ctx context.Context,
	socket string,
	request sourceControlRequest,
) (sourceControlResponse, []byte, []byte, error) {
	// Every source-control transaction owns a hard deadline. HTTP request
	// contexts normally carry cancellation but no deadline, and cancellation
	// alone does not interrupt a Unix socket read after DialContext succeeds.
	timeout := sourceControlRequestTimeout
	if request.Operation == "snapshot" {
		// A 4K snapshot includes JPEG plus roughly 24 MiB of exact RGB data.
		// Keep it bounded without imposing the low-latency lifecycle deadline.
		timeout = sourceSnapshotRequestTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	dialer := net.Dialer{}
	connection, err := dialer.DialContext(ctx, "unix", socket)
	if err != nil {
		return sourceControlResponse{}, nil, nil, fmt.Errorf("connect capture source: %w", err)
	}
	defer connection.Close()
	if deadline, found := ctx.Deadline(); found {
		_ = connection.SetDeadline(deadline)
	}
	stopCancellation := context.AfterFunc(ctx, func() {
		_ = connection.SetDeadline(time.Now())
	})
	defer stopCancellation()
	if err := json.NewEncoder(connection).Encode(request); err != nil {
		return sourceControlResponse{}, nil, nil, fmt.Errorf("write capture source request: %w", err)
	}
	reader := bufio.NewReader(connection)
	header, err := reader.ReadString('\n')
	if err != nil {
		return sourceControlResponse{}, nil, nil, fmt.Errorf("read capture source response: %w", err)
	}
	if len(header) > maximumControlHeaderBytes {
		return sourceControlResponse{}, nil, nil, errors.New("capture source response header is too large")
	}
	var response sourceControlResponse
	if err := json.Unmarshal([]byte(header), &response); err != nil {
		return sourceControlResponse{}, nil, nil, fmt.Errorf("decode capture source response: %w", err)
	}
	if !response.OK {
		message := strings.TrimSpace(response.Error)
		if message == "" {
			message = "capture source rejected the request"
		}
		return sourceControlResponse{}, nil, nil, errors.New(message)
	}
	if request.Operation != "snapshot" {
		return response, nil, nil, nil
	}
	includeRGB := request.IncludeRGB == nil || *request.IncludeRGB
	if response.JPEGBytes < 2 || response.JPEGBytes > 32<<20 ||
		(includeRGB && (response.RGBBytes < 1 || response.RGBBytes > 128<<20)) ||
		(!includeRGB && response.RGBBytes != 0 &&
			(response.RGBBytes < 1 || response.RGBBytes > 128<<20)) {
		return sourceControlResponse{}, nil, nil, errors.New("capture source snapshot sizes are invalid")
	}
	jpeg := make([]byte, response.JPEGBytes)
	if _, err := io.ReadFull(reader, jpeg); err != nil {
		return sourceControlResponse{}, nil, nil, fmt.Errorf("read capture JPEG: %w", err)
	}
	rgb := make([]byte, response.RGBBytes)
	if _, err := io.ReadFull(reader, rgb); err != nil {
		return sourceControlResponse{}, nil, nil, fmt.Errorf("read capture RGB: %w", err)
	}
	return response, jpeg, rgb, nil
}

func newSnapshotID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("create snapshot ID: %w", err)
	}
	return hex.EncodeToString(value), nil
}
