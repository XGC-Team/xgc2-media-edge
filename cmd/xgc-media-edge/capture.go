package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"time"

	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/httpx"
)

// Capture is an ordinary finite native command. The workflow passes the actual
// service reference and destination; no source catalog or endpoint lives in Core.
type captureInput struct {
	Service       xrpc.ServiceRef `json:"service"`
	SourceID      string          `json:"sourceId"`
	ArtifactRoot  string          `json:"artifactRoot"`
	ImageFormat   string          `json:"imageFormat"`
	IncludeRawRGB bool            `json:"includeRawRGB"`
	Label         string          `json:"label"`
}

func captureCommand(ctx context.Context, input io.Reader, output io.Writer) (result error) {
	var config captureInput
	data, err := io.ReadAll(io.LimitReader(input, (64<<10)+1))
	if err != nil {
		return err
	}
	if len(data) > 64<<10 {
		return errors.New("capture input exceeds 64 KiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("capture requires one JSON input")
	}
	if err := config.Service.ValidateInternal(); err != nil {
		return err
	}
	if config.Service.Service != "media-edge" || config.Service.APIVersion != "v1" || config.Service.Profile != xrpc.HTTP {
		return errors.New("capture requires a media-edge/v1 HTTP service reference")
	}
	if !regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`).MatchString(config.SourceID) || len(config.Label) > 120 {
		return errors.New("invalid capture source or label")
	}
	if config.ImageFormat == "" {
		config.ImageFormat = "jpeg"
	}
	if config.ImageFormat != "jpeg" && config.ImageFormat != "png" {
		return errors.New("imageFormat must be jpeg or png")
	}
	if !filepath.IsAbs(config.ArtifactRoot) || filepath.Clean(config.ArtifactRoot) != config.ArtifactRoot {
		return errors.New("artifactRoot must be canonical and absolute")
	}
	resolved, err := filepath.EvalSymlinks(config.ArtifactRoot)
	if err != nil || resolved != config.ArtifactRoot {
		return errors.New("artifactRoot must be an existing directory without symlinks")
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.IsDir() {
		return errors.New("artifactRoot must be a directory")
	}
	client, err := httpx.New(httpx.Config{LocalTargetID: config.Service.TargetID, Service: config.Service,
		MaxConnections: 1, MaxInFlight: 1, MaxResponseBytes: (160 << 20) + (128 << 10), MaxHeaderBytes: 16 << 10})
	if err != nil {
		return err
	}
	defer client.Close()
	var identity [16]byte
	if _, err := rand.Read(identity[:]); err != nil {
		return err
	}
	requestID := "capture:" + hex.EncodeToString(identity[:])
	request, _ := json.Marshal(map[string]bool{"includeRgb": config.IncludeRawRGB || config.ImageFormat == "png", "requestKeyframe": true, "requireFresh": true})
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	reply, err := client.DoStream(ctx, http.MethodPost, "/v1/media/sources/"+config.SourceID+"/capture", requestID, "application/json", request)
	if err != nil {
		return err
	}
	defer reply.Body.Close()
	if reply.StatusCode != http.StatusOK {
		return fmt.Errorf("capture returned HTTP %d", reply.StatusCode)
	}
	kind, parameters, err := mime.ParseMediaType(reply.Header.Get("Content-Type"))
	if err != nil || kind != "multipart/mixed" || len(parameters["boundary"]) == 0 || len(parameters["boundary"]) > 70 {
		return errors.New("invalid capture multipart response")
	}
	phase := &io.LimitedReader{R: reply.Body}
	reader := multipart.NewReader(phase, parameters["boundary"])
	next := func(name, kind string, limit int64) ([]byte, error) {
		phase.N = 16 << 10
		part, err := reader.NextRawPart()
		if err != nil {
			return nil, err
		}
		if len(part.Header.Values("Content-Disposition")) != 1 || len(part.Header.Values("Content-Type")) != 1 {
			return nil, errors.New("duplicate or missing capture part headers")
		}
		disposition, fields, err := mime.ParseMediaType(part.Header.Get("Content-Disposition"))
		if err != nil || disposition != "inline" || len(fields) != 1 || fields["name"] != name || part.Header.Get("Content-Type") != kind {
			return nil, errors.New("invalid capture part")
		}
		phase.N = limit + 4096
		data, err := io.ReadAll(io.LimitReader(part, limit+1))
		if err != nil {
			return nil, err
		}
		if int64(len(data)) > limit {
			return nil, errors.New("capture part exceeds byte budget")
		}
		return data, nil
	}
	metadata, err := next("metadata", "application/json", 64<<10)
	if err != nil {
		return err
	}
	var frame struct {
		SnapshotID           string `json:"snapshotId"`
		SourceID             string `json:"sourceId"`
		FrameID              string `json:"frameId"`
		TimestampNanoseconds int64  `json:"timestampNanoseconds"`
		TimestampClockDomain string `json:"timestampClockDomain"`
		Width                int    `json:"width"`
		Height               int    `json:"height"`
		JPEGBytes            int64  `json:"jpegBytes"`
		RGBBytes             int64  `json:"rgbBytes"`
	}
	if err = json.Unmarshal(metadata, &frame); err != nil {
		return err
	}
	if !regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`).MatchString(frame.SnapshotID) || frame.SourceID != config.SourceID || frame.Width <= 0 || frame.Height <= 0 || frame.Width > 16384 || frame.Height > 16384 || frame.JPEGBytes < 2 || frame.JPEGBytes > 32<<20 || frame.RGBBytes < 0 || frame.RGBBytes > 128<<20 {
		return errors.New("invalid capture identity, dimensions or byte counts")
	}
	defer func() {
		_ = reply.Body.Close()
		cleanup, done := context.WithTimeout(context.Background(), 2*time.Second)
		defer done()
		_, status, _, err := client.Do(cleanup, http.MethodDelete, "/v1/media/snapshots/"+frame.SnapshotID, requestID+":release", "", nil)
		if err != nil || status != http.StatusNoContent {
			result = errors.Join(result, fmt.Errorf("capture retention DELETE: HTTP %d: %w", status, err))
		}
	}()
	jpegBytes, err := next("jpeg", "image/jpeg", frame.JPEGBytes)
	if err != nil {
		return err
	}
	if int64(len(jpegBytes)) != frame.JPEGBytes {
		return errors.New("capture JPEG length differs from metadata")
	}
	dimensions, err := jpeg.DecodeConfig(bytes.NewReader(jpegBytes))
	if err != nil || dimensions.Width != frame.Width || dimensions.Height != frame.Height {
		return errors.New("capture JPEG dimensions differ from metadata")
	}
	var rgb []byte
	if config.IncludeRawRGB || config.ImageFormat == "png" {
		if frame.RGBBytes != int64(frame.Width)*int64(frame.Height)*3 {
			return errors.New("capture RGB length differs from dimensions")
		}
		rgb, err = next("rgb", "application/octet-stream", frame.RGBBytes)
		if err != nil {
			return err
		}
		if int64(len(rgb)) != frame.RGBBytes {
			return errors.New("capture RGB length differs from metadata")
		}
	} else if frame.RGBBytes != 0 {
		return errors.New("capture returned unrequested RGB")
	}
	phase.N = 16 << 10
	if _, err := reader.NextRawPart(); err != io.EOF {
		return errors.New("unexpected capture multipart tail")
	}
	parent := filepath.Join(config.ArtifactRoot, "camera-snapshots")
	if err := os.MkdirAll(parent, 0750); err != nil {
		return err
	}
	if real, err := filepath.EvalSymlinks(parent); err != nil || real != parent {
		return errors.New("capture directory contains a symlink")
	}
	stage, err := os.MkdirTemp(parent, ".capture-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stage)
	final := filepath.Join(parent, filepath.Base(stage)[1:])
	imageName, imageType := "image.jpg", "image/jpeg"
	imageData := jpegBytes
	if config.ImageFormat == "png" {
		var encoded bytes.Buffer
		if err := png.Encode(&encoded, rgbImage{rgb, frame.Width, frame.Height}); err != nil {
			return err
		}
		imageName, imageType, imageData = "image.png", "image/png", encoded.Bytes()
	}
	write := func(name string, data []byte) error {
		file, err := os.OpenFile(filepath.Join(stage, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0640)
		if err != nil {
			return err
		}
		_, err = file.Write(data)
		if err == nil {
			err = file.Sync()
		}
		return errors.Join(err, file.Close())
	}
	if err := write(imageName, imageData); err != nil {
		return err
	}
	digest := func(data []byte) string { value := sha256.Sum256(data); return hex.EncodeToString(value[:]) }
	fileRecord := func(name, kind string, data []byte) map[string]any {
		return map[string]any{"path": name, "mediaType": kind, "sizeBytes": len(data), "sha256": digest(data)}
	}
	manifest := map[string]any{"schemaVersion": 1, "label": config.Label, "sourceId": config.SourceID, "capturedAtSystemTime": time.Now().UTC(), "snapshot": json.RawMessage(metadata), "image": fileRecord(imageName, imageType, imageData)}
	if config.IncludeRawRGB {
		if err := write("frame.rgb8", rgb); err != nil {
			return err
		}
		manifest["rawRgb"] = fileRecord("frame.rgb8", "application/x-xgc-rgb8", rgb)
	}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	if err := write("manifest.json", manifestBytes); err != nil {
		return err
	}
	syncDirectory := func(path string) error {
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		return errors.Join(file.Sync(), file.Close())
	}
	if err := syncDirectory(stage); err != nil {
		return err
	}
	if err := os.Rename(stage, final); err != nil {
		return err
	}
	if err := syncDirectory(parent); err != nil {
		return err
	}
	value := map[string]any{"snapshotId": frame.SnapshotID, "sourceId": frame.SourceID, "frameId": frame.FrameID,
		"timestampNanoseconds": frame.TimestampNanoseconds, "timestampClockDomain": frame.TimestampClockDomain,
		"imagePath": filepath.Join(final, imageName), "metadataPath": filepath.Join(final, "manifest.json"), "imageSha256": digest(imageData)}
	if config.IncludeRawRGB {
		value["rawRgbPath"] = filepath.Join(final, "frame.rgb8")
	}
	return json.NewEncoder(output).Encode(value)
}

type rgbImage struct {
	data          []byte
	width, height int
}

func (v rgbImage) ColorModel() color.Model { return color.NRGBAModel }
func (v rgbImage) Bounds() image.Rectangle { return image.Rect(0, 0, v.width, v.height) }
func (v rgbImage) At(x, y int) color.Color {
	i := (y*v.width + x) * 3
	return color.NRGBA{v.data[i], v.data[i+1], v.data[i+2], 255}
}
