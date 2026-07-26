package mediaedge

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type httpServer struct {
	server *Server
	http   *http.Server
}

func newHTTPServer(server *Server) *httpServer {
	httpServer := &httpServer{server: server}
	httpServer.http = &http.Server{
		Handler:           http.HandlerFunc(httpServer.route),
		ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      20 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	return httpServer
}

func (server *httpServer) serve(listener net.Listener) error {
	err := server.http.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (server *httpServer) close() error {
	if server == nil || server.http == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return server.http.Shutdown(ctx)
}

func (server *httpServer) route(writer http.ResponseWriter, request *http.Request) {
	if !requestFromLoopback(request) {
		writeError(writer, http.StatusForbidden, "media edge control is loopback-only")
		return
	}
	path := strings.Trim(strings.TrimSpace(request.URL.Path), "/")
	parts := strings.Split(path, "/")
	switch {
	case request.Method == http.MethodGet && path == "healthz":
		writeJSON(writer, http.StatusOK, struct {
			Sources []SourceStatus `json:"sources"`
		}{Sources: server.server.SourceStatuses()})
		return
	case request.Method == http.MethodPost && len(parts) == 5 &&
		parts[0] == "api" && parts[1] == "v1" && parts[2] == "sources" && parts[4] == "sessions":
		server.openSession(writer, request, parts[3])
		return
	case request.Method == http.MethodDelete && len(parts) == 4 &&
		parts[0] == "api" && parts[1] == "v1" && parts[2] == "sessions":
		server.closeSession(writer, parts[3])
		return
	case request.Method == http.MethodPost && len(parts) == 5 &&
		parts[0] == "api" && parts[1] == "v1" && parts[2] == "sources" && parts[4] == "snapshots":
		server.captureSnapshot(writer, request, parts[3])
		return
	case request.Method == http.MethodGet && len(parts) == 5 &&
		parts[0] == "api" && parts[1] == "v1" && parts[2] == "snapshots" && parts[4] == "raw":
		server.readSnapshotRaw(writer, parts[3])
		return
	case request.Method == http.MethodGet && len(parts) == 5 &&
		parts[0] == "api" && parts[1] == "v1" && parts[2] == "snapshots" && parts[4] == "jpeg":
		server.readSnapshotJPEG(writer, parts[3])
		return
	case request.Method == http.MethodDelete && len(parts) == 4 &&
		parts[0] == "api" && parts[1] == "v1" && parts[2] == "snapshots":
		server.deleteSnapshot(writer, parts[3])
		return
	default:
		writeError(writer, http.StatusNotFound, "media edge endpoint was not found")
	}
}

func (server *httpServer) openSession(writer http.ResponseWriter, request *http.Request, sourceID string) {
	var offer SessionOffer
	if !decodeJSON(writer, request, &offer) {
		return
	}
	answer, err := server.server.OpenSession(request.Context(), sourceID, offer)
	if err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "was not found") {
			status = http.StatusNotFound
		}
		if strings.Contains(err.Error(), "activate media source") {
			status = http.StatusServiceUnavailable
		}
		writeError(writer, status, err.Error())
		return
	}
	writeJSON(writer, http.StatusCreated, answer)
}

func (server *httpServer) closeSession(writer http.ResponseWriter, sessionID string) {
	if !server.server.CloseSession(sessionID) {
		writeError(writer, http.StatusNotFound, "media session was not found")
		return
	}
	writer.WriteHeader(http.StatusNoContent)
}

func (server *httpServer) captureSnapshot(writer http.ResponseWriter, request *http.Request, sourceID string) {
	snapshot, err := server.server.CaptureSnapshot(request.Context(), sourceID)
	if err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "was not found") {
			status = http.StatusNotFound
		} else if strings.Contains(err.Error(), "activate media source") || strings.Contains(err.Error(), "capture source") {
			status = http.StatusServiceUnavailable
		}
		writeError(writer, status, err.Error())
		return
	}
	writeJSON(writer, http.StatusCreated, snapshot.metadata())
}

func (server *httpServer) readSnapshotRaw(writer http.ResponseWriter, snapshotID string) {
	snapshot, found := server.server.Snapshot(snapshotID)
	if !found {
		writeError(writer, http.StatusNotFound, "media snapshot was not found")
		return
	}
	writer.Header().Set("Content-Type", "application/x-xgc-rgb8")
	writer.Header().Set("Content-Length", strconv.Itoa(len(snapshot.RGB)))
	writer.Header().Set("X-XGC-Snapshot-Id", snapshot.ID)
	writer.Header().Set("X-XGC-Frame-Id", snapshot.FrameID)
	writer.Header().Set("X-XGC-Width", strconv.Itoa(snapshot.Width))
	writer.Header().Set("X-XGC-Height", strconv.Itoa(snapshot.Height))
	_, _ = writer.Write(snapshot.RGB)
}

func (server *httpServer) readSnapshotJPEG(writer http.ResponseWriter, snapshotID string) {
	snapshot, found := server.server.Snapshot(snapshotID)
	if !found {
		writeError(writer, http.StatusNotFound, "media snapshot was not found")
		return
	}
	writer.Header().Set("Content-Type", "image/jpeg")
	writer.Header().Set("Content-Length", strconv.Itoa(len(snapshot.JPEG)))
	writer.Header().Set("Cache-Control", "no-store")
	_, _ = writer.Write(snapshot.JPEG)
}

func (server *httpServer) deleteSnapshot(writer http.ResponseWriter, snapshotID string) {
	if !server.server.DeleteSnapshot(snapshotID) {
		writeError(writer, http.StatusNotFound, "media snapshot was not found")
		return
	}
	writer.WriteHeader(http.StatusNoContent)
}

func requestFromLoopback(request *http.Request) bool {
	host, _, err := net.SplitHostPort(request.RemoteAddr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func decodeJSON(writer http.ResponseWriter, request *http.Request, target any) bool {
	if request.Body == nil {
		writeError(writer, http.StatusBadRequest, "JSON request body is required")
		return false
	}
	deferred := http.MaxBytesReader(writer, request.Body, 300<<10)
	defer deferred.Close()
	decoder := json.NewDecoder(deferred)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeError(writer, http.StatusBadRequest, "invalid JSON request")
		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeError(writer, http.StatusBadRequest, "JSON request must contain one value")
		return false
	}
	return true
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-store")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}

func writeError(writer http.ResponseWriter, status int, message string) {
	writeJSON(writer, status, struct {
		Error string `json:"error"`
	}{Error: message})
}
