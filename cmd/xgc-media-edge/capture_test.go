package main

import (
	"bytes"
	"context"
	"encoding/json"
	"image"
	"image/jpeg"
	"image/png"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/httpx"
)

func TestCaptureCommandSavesSameFrameAndReleasesNativeRetention(t *testing.T) {
	root := t.TempDir()
	socket := filepath.Join(root, "media.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	ref := xrpc.ServiceRef{TargetID: "fixture", Service: "media-edge", APIVersion: "v1", InstanceID: "capture-fixture", Profile: xrpc.HTTP, Endpoint: xrpc.Endpoint{Kind: "unix", Address: socket}}
	rgb := []byte{255, 0, 0, 0, 255, 0, 0, 0, 255, 255, 255, 255}
	var jpegData bytes.Buffer
	if err := jpeg.Encode(&jpegData, rgbImage{rgb, 2, 2}, nil); err != nil {
		t.Fatal(err)
	}
	var captures, released atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			released.Add(1)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/v1/media/sources/camera/capture" {
			t.Errorf("unexpected capture request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(400)
			return
		}
		var options struct {
			IncludeRGB bool `json:"includeRgb"`
		}
		if err := json.NewDecoder(r.Body).Decode(&options); err != nil {
			t.Error(err)
		}
		captures.Add(1)
		metadata, _ := json.Marshal(map[string]any{"snapshotId": "frame-1", "sourceId": "camera", "frameId": "optical", "timestampNanoseconds": int64(1700000000000000001), "timestampClockDomain": "simulation", "width": 2, "height": 2, "jpegBytes": jpegData.Len(), "rgbBytes": map[bool]int{true: len(rgb)}[options.IncludeRGB]})
		stream := multipart.NewWriter(w)
		w.Header().Set("Content-Type", "multipart/mixed; boundary="+stream.Boundary())
		for _, part := range []struct {
			name, kind string
			data       []byte
		}{{"metadata", "application/json", metadata}, {"jpeg", "image/jpeg", jpegData.Bytes()}, {"rgb", "application/octet-stream", rgb}} {
			if part.name == "rgb" && !options.IncludeRGB {
				continue
			}
			header := textproto.MIMEHeader{"Content-Type": {part.kind}, "Content-Disposition": {`inline; name="` + part.name + `"`}}
			writer, err := stream.CreatePart(header)
			if err != nil {
				t.Error(err)
				return
			}
			if _, err := writer.Write(part.data); err != nil {
				t.Error(err)
				return
			}
		}
		if err := stream.Close(); err != nil {
			t.Error(err)
		}
	})
	server := httptest.NewUnstartedServer(httpx.Handler(handler, httpx.HostOptions{InstanceID: ref.InstanceID}))
	server.Listener = listener
	server.Start()
	defer server.Close()
	for _, format := range []string{"jpeg", "png"} {
		t.Run(format, func(t *testing.T) {
			input, _ := json.Marshal(captureInput{Service: ref, SourceID: "camera", ArtifactRoot: root, ImageFormat: format, IncludeRawRGB: format == "png"})
			var stdout bytes.Buffer
			if err := captureCommand(context.Background(), bytes.NewReader(input), &stdout); err != nil {
				t.Fatal(err)
			}
			var saved struct {
				ImagePath            string
				MetadataPath         string
				RawRGBPath           string
				TimestampNanoseconds int64
			}
			if err := json.Unmarshal(stdout.Bytes(), &saved); err != nil {
				t.Fatal(err)
			}
			if saved.TimestampNanoseconds != 1700000000000000001 {
				t.Fatal("source timestamp changed")
			}
			file, err := os.Open(saved.ImagePath)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			var decoded image.Image
			if format == "png" {
				decoded, err = png.Decode(file)
			} else {
				decoded, err = jpeg.Decode(file)
			}
			if err != nil || decoded.Bounds() != image.Rect(0, 0, 2, 2) {
				t.Fatalf("saved image: %v", err)
			}
			if format == "png" {
				raw, err := os.ReadFile(saved.RawRGBPath)
				if err != nil || !bytes.Equal(raw, rgb) {
					t.Fatal("same-frame RGB changed")
				}
			}
			if _, err := os.Stat(saved.MetadataPath); err != nil {
				t.Fatal(err)
			}
		})
	}
	if captures.Load() != 2 || released.Load() != 2 {
		t.Fatalf("native captures=%d retained releases=%d", captures.Load(), released.Load())
	}
	ref.InstanceID = "retired-instance"
	input, _ := json.Marshal(captureInput{Service: ref, SourceID: "camera", ArtifactRoot: root})
	if err := captureCommand(context.Background(), bytes.NewReader(input), new(bytes.Buffer)); err == nil {
		t.Fatal("stale service reference accepted")
	}
	if captures.Load() != 2 {
		t.Fatal("stale reference reached capture")
	}
	if err := captureCommand(context.Background(), strings.NewReader(strings.Repeat(" ", 64<<10)+"{}"), new(bytes.Buffer)); err == nil {
		t.Fatal("oversize input accepted")
	}
}
