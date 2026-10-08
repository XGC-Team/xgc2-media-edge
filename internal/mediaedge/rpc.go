package mediaedge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"os"
	"strings"

	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/httpx"
	unixlease "github.com/XGC-Team/xgc2-xrpc/go/unix"
)

func (server *MediaMTXServer) startRPC() error {
	if server.config.RPCSocket == "" {
		return nil
	}
	target, err := os.Hostname()
	if err != nil {
		return err
	}
	server.instanceID, err = newSnapshotID()
	if err != nil {
		return err
	}
	lease, err := unixlease.Reserve(server.lifecycleContext, server.config.RPCSocket, unixlease.Options{ExistingPath: unixlease.ReclaimUnreachable})
	if err != nil {
		return fmt.Errorf("reserve media edge XRPC: %w", err)
	}
	listener, err := lease.Listen()
	if err != nil {
		_ = lease.Close()
		return err
	}
	reference := xrpc.ServiceRef{TargetID: target, Service: "media-edge", APIVersion: "v1", InstanceID: server.instanceID, Profile: xrpc.HTTP, Endpoint: xrpc.Endpoint{Kind: "unix", Address: server.config.RPCSocket}}
	options, err := server.config.hostOptions(true)
	if err != nil {
		_ = listener.Close()
		_ = lease.Close()
		return err
	}
	options.InstanceID = server.instanceID
	options.DiscoveryPaths = []string{"/v1/describe"}
	server.rpcHost, err = httpx.Serve(listener, lease, http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodGet && request.URL.Path == "/v1/describe" {
			writeJSON(writer, http.StatusOK, map[string]any{"service_ref": reference, "sources": server.SourceStatuses(), "capture": "/v1/media/sources/{sourceId}/capture", "runtime_policy": server.config.effectivePolicy(), "limits": map[string]int{"max_sources": maximumSources, "max_sessions": server.config.MaxSessions, "max_operations": server.config.MaxOperations, "max_captures_per_source": 1, "snapshots_per_source": maximumSnapshots, "jpeg_bytes": maximumCameraJPEGBytes, "rgb_bytes": maximumCameraRGBBytes}})
			return
		}
		parts := strings.Split(strings.Trim(request.URL.Path, "/"), "/")
		if request.Method == http.MethodGet && request.URL.Path == "/v1/health" {
			writeJSON(writer, http.StatusOK, map[string]any{"closing": server.isClosing(), "sources": server.SourceStatuses()})
			return
		}
		if len(parts) == 5 && parts[0] == "v1" && parts[1] == "media" && parts[2] == "sources" && parts[4] == "capture" && request.Method == http.MethodPost {
			var input SnapshotCaptureRequest
			if !decodeJSON(writer, request, &input) {
				return
			}
			snapshot, err := server.CaptureSnapshot(request.Context(), parts[3], input)
			if err != nil {
				status := http.StatusServiceUnavailable
				if errors.Is(err, ErrMediaCapacity) {
					status = http.StatusTooManyRequests
				}
				if server.source(parts[3]) == nil {
					status = http.StatusNotFound
				}
				writeError(writer, status, err.Error())
				return
			}
			// Native MIME writes immutable buffers one part at a time; no Base64 and
			// no whole-frame join copy. The SDK owns IO deadline and caller admission.
			if err := writeSnapshotMultipart(writer, snapshot); err != nil {
				panic(http.ErrAbortHandler)
			}
			return
		}
		if len(parts) == 4 && parts[0] == "v1" && parts[1] == "media" && parts[2] == "snapshots" && request.Method == http.MethodDelete {
			if !server.DeleteSnapshot(parts[3]) {
				writeError(writer, http.StatusNotFound, "media snapshot was not found")
				return
			}
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		writeError(writer, http.StatusNotFound, "media edge XRPC endpoint was not found")
	}), options)
	if err != nil {
		_ = listener.Close()
		_ = lease.Close()
	}
	return err
}

func (server *MediaMTXServer) closeRPC() error {
	ctx, cancel := context.WithTimeout(context.Background(), server.shutdownTimeout())
	defer cancel()
	err := server.rpcHost.Shutdown(ctx)
	select {
	case <-server.rpcHost.Drained():
		return server.rpcHost.Wait()
	default:
		return err
	}
}

func writeSnapshotMultipart(writer http.ResponseWriter, snapshot Snapshot) error {
	metadata, err := json.Marshal(snapshot.metadata())
	if err != nil {
		return err
	}
	if len(metadata) > maximumControlHeaderBytes {
		return errors.New("snapshot metadata exceeds byte limit")
	}
	stream := multipart.NewWriter(writer)
	writer.Header().Set("Content-Type", "multipart/mixed; boundary="+stream.Boundary())
	writer.Header().Set("Cache-Control", "no-store")
	for _, item := range []struct {
		name, kind string
		data       []byte
	}{{"metadata", "application/json", metadata}, {"jpeg", "image/jpeg", snapshot.JPEG}, {"rgb", "application/octet-stream", snapshot.RGB}} {
		if item.name == "rgb" && len(item.data) == 0 {
			continue
		}
		header := textproto.MIMEHeader{}
		header.Set("Content-Type", item.kind)
		header.Set("Content-Disposition", `inline; name="`+item.name+`"`)
		part, err := stream.CreatePart(header)
		if err != nil {
			return err
		}
		if _, err = part.Write(item.data); err != nil {
			return err
		}
	}
	return stream.Close()
}
