// capture_client shows one local, instance-bound SDK capture and retention DELETE.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"time"

	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/httpx"
)

const maxJPEG = 32 << 20
const maxMetadata = 64 << 10
const maxPartHeader = 16 << 10

func main() {
	socket := flag.String("rpc-socket", "", "granted media-edge Unix endpoint")
	source := flag.String("source", "", "configured source ID")
	output := flag.String("output", "capture.jpg", "JPEG output; same-frame metadata goes to output.json")
	flag.Parse()
	if err := capture(*socket, *source, *output); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func capture(socket, source, output string) (result error) {
	if socket == "" || !regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`).MatchString(source) {
		return errors.New("rpc-socket and valid source are required")
	}
	target, err := os.Hostname()
	if err != nil {
		return err
	}
	ref := xrpc.ServiceRef{TargetID: target, Service: "media-edge", APIVersion: "v1", Profile: xrpc.HTTP, Endpoint: xrpc.Endpoint{Kind: "unix", Address: socket}}
	newClient := func(reference xrpc.ServiceRef) (*httpx.Client, error) {
		return httpx.New(httpx.Config{LocalTargetID: target, Service: reference, MaxConnections: 1, MaxInFlight: 1, MaxResponseBytes: maxJPEG + maxMetadata + 4*maxPartHeader, MaxHeaderBytes: maxPartHeader})
	}
	client, err := newClient(ref)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	data, status, _, err := client.Do(ctx, http.MethodGet, "/v1/describe", "capture-example:discover", "", nil)
	client.Close()
	if err != nil || status != 200 {
		return fmt.Errorf("discover: HTTP %d: %w", status, err)
	}
	var discovery struct {
		Service xrpc.ServiceRef `json:"service_ref"`
	}
	if err := json.Unmarshal(data, &discovery); err != nil {
		return err
	}
	bound := discovery.Service
	if bound.InstanceID == "" || bound.Endpoint != ref.Endpoint || bound.TargetID != target || bound.Service != "media-edge" || bound.APIVersion != "v1" {
		return errors.New("discovery did not identify this local media-edge endpoint")
	}
	client, err = newClient(bound)
	if err != nil {
		return err
	}
	defer client.Close()
	reply, err := client.DoStream(ctx, http.MethodPost, "/v1/media/sources/"+source+"/capture", "capture-example:capture", "application/json", []byte(`{"includeRgb":false,"requestKeyframe":false,"requireFresh":true}`))
	if err != nil {
		return err
	}
	defer reply.Body.Close()
	if reply.StatusCode != 200 {
		return fmt.Errorf("capture: HTTP %d", reply.StatusCode)
	}
	kind, parameters, err := mime.ParseMediaType(reply.Header.Get("Content-Type"))
	if err != nil || kind != "multipart/mixed" || len(parameters["boundary"]) == 0 || len(parameters["boundary"]) > 70 {
		return errors.New("invalid native multipart response")
	}
	// Limit native header phases before parsing. multipart may read ahead 4096
	// bytes; exact declared binary lengths separately govern payload reads.
	phase := &io.LimitedReader{R: reply.Body}
	reader := multipart.NewReader(phase, parameters["boundary"])
	next := func() (*multipart.Part, error) { phase.N = maxPartHeader; return reader.NextRawPart() }
	part, err := next()
	if err != nil {
		return err
	}
	if err := checkPart(part, "metadata", "application/json", -1); err != nil {
		return err
	}
	phase.N = maxMetadata + 4096
	metadata, err := io.ReadAll(io.LimitReader(part, maxMetadata+1))
	if err != nil || len(metadata) > maxMetadata {
		return errors.New("metadata exceeds 64 KiB")
	}
	if err := checkPart(part, "metadata", "application/json", int64(len(metadata))); err != nil {
		return err
	}
	var frame struct {
		SnapshotID string `json:"snapshotId"`
		SourceID   string `json:"sourceId"`
		Sequence   uint64 `json:"frameSequence"`
		JPEGBytes  int64  `json:"jpegBytes"`
		RGBBytes   int64  `json:"rgbBytes"`
	}
	if err := json.Unmarshal(metadata, &frame); err != nil {
		return err
	}
	if !regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`).MatchString(frame.SnapshotID) || frame.SourceID != source || frame.Sequence == 0 || frame.JPEGBytes < 2 || frame.JPEGBytes > maxJPEG || frame.RGBBytes != 0 {
		return errors.New("capture identity or JPEG-only lengths are invalid")
	}
	// Deletion is a separate explicit operation after consuming/closing the
	// response; it never retries an outcome-unknown capture POST.
	defer func() {
		_ = reply.Body.Close()
		cleanup, done := context.WithTimeout(context.Background(), 2*time.Second)
		defer done()
		_, status, _, err := client.Do(cleanup, http.MethodDelete, "/v1/media/snapshots/"+frame.SnapshotID, "capture-example:delete", "", nil)
		if err != nil || status != http.StatusNoContent {
			result = errors.Join(result, fmt.Errorf("retention DELETE: HTTP %d: %w", status, err))
		}
	}()
	part, err = next()
	if err != nil {
		return err
	}
	if err := checkPart(part, "jpeg", "image/jpeg", frame.JPEGBytes); err != nil {
		return err
	}
	file, err := os.OpenFile(output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	phase.N = frame.JPEGBytes + 4096
	_, copyErr := io.CopyN(file, part, frame.JPEGBytes)
	closeErr := file.Close()
	if copyErr != nil || closeErr != nil {
		return errors.Join(copyErr, closeErr)
	}
	var extra [1]byte
	if n, err := part.Read(extra[:]); n != 0 || err != io.EOF {
		return errors.New("JPEG size does not equal metadata")
	}
	if _, err := next(); err != io.EOF {
		return errors.New("unexpected native multipart tail")
	}
	if err := os.WriteFile(output+".json", metadata, 0600); err != nil {
		return err
	}
	fmt.Printf("saved source=%s frameSequence=%d snapshotId=%s\n", frame.SourceID, frame.Sequence, frame.SnapshotID)
	return nil
}

func checkPart(part *multipart.Part, name, kind string, size int64) error {
	if len(part.Header.Values("Content-Type")) != 1 || part.Header.Get("Content-Type") != kind || len(part.Header.Values("Content-Disposition")) != 1 {
		return errors.New("invalid native multipart headers")
	}
	disposition, parameters, err := mime.ParseMediaType(part.Header.Get("Content-Disposition"))
	if err != nil || disposition != "inline" || len(parameters) != 1 || parameters["name"] != name {
		return errors.New("unexpected native multipart part")
	}
	for key, values := range part.Header {
		if key == "Content-Type" || key == "Content-Disposition" {
			continue
		}
		if key != "Content-Length" || len(values) != 1 {
			return errors.New("unsupported native multipart header")
		}
		if size >= 0 && values[0] != strconv.FormatInt(size, 10) {
			return errors.New("part Content-Length differs from capture metadata")
		}
	}
	return nil
}
