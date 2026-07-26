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
	"net"
	"strings"
	"time"
)

const maximumControlHeaderBytes = 64 << 10

type sourceControlRequest struct {
	Operation  string `json:"operation"`
	SnapshotID string `json:"snapshotId,omitempty"`
	Active     *bool  `json:"active,omitempty"`
}

type sourceControlResponse struct {
	OK         bool   `json:"ok"`
	Error      string `json:"error,omitempty"`
	SnapshotID string `json:"snapshotId,omitempty"`
	FrameID    string `json:"frameId,omitempty"`
	// TimestampNanoseconds is in the source clock domain. A Gazebo source uses
	// simulation time, so calling it UnixNano would be materially incorrect.
	TimestampNanoseconds int64     `json:"timestampNanoseconds,omitempty"`
	Width                int       `json:"width,omitempty"`
	Height               int       `json:"height,omitempty"`
	PixelFormat          string    `json:"pixelFormat,omitempty"`
	JPEGBytes            int       `json:"jpegBytes,omitempty"`
	RGBBytes             int       `json:"rgbBytes,omitempty"`
	CameraMatrix         []float64 `json:"cameraMatrix,omitempty"`
	Distortion           []float64 `json:"distortion,omitempty"`
}

// Snapshot is immutable data owned by the media edge. JPEG is for an operator
// display, while RGB is the exact same frame for local calibration algorithms.
// The two representations are created by one source-side snapshot transaction.
type Snapshot struct {
	ID                   string
	SourceID             string
	FrameID              string
	TimestampNanoseconds int64
	Width                int
	Height               int
	PixelFormat          string
	JPEG                 []byte
	RGB                  []byte
	CameraMatrix         []float64
	Distortion           []float64
	ExpiresAt            time.Time
}

func (snapshot Snapshot) metadata() snapshotMetadata {
	return snapshotMetadata{
		SnapshotID: snapshot.ID, SourceID: snapshot.SourceID, FrameID: snapshot.FrameID,
		TimestampNanoseconds: snapshot.TimestampNanoseconds, Width: snapshot.Width, Height: snapshot.Height,
		PixelFormat: snapshot.PixelFormat, JPEGBytes: len(snapshot.JPEG), CameraMatrix: append([]float64(nil), snapshot.CameraMatrix...),
		Distortion: append([]float64(nil), snapshot.Distortion...),
	}
}

type snapshotMetadata struct {
	SnapshotID           string    `json:"snapshotId"`
	SourceID             string    `json:"sourceId"`
	FrameID              string    `json:"frameId"`
	TimestampNanoseconds int64     `json:"timestampNanoseconds"`
	Width                int       `json:"width"`
	Height               int       `json:"height"`
	PixelFormat          string    `json:"pixelFormat"`
	JPEGBytes            int       `json:"jpegBytes"`
	CameraMatrix         []float64 `json:"cameraMatrix"`
	Distortion           []float64 `json:"distortion"`
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
	if response.JPEGBytes < 2 || response.JPEGBytes > 32<<20 ||
		response.RGBBytes < 1 || response.RGBBytes > 128<<20 {
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
