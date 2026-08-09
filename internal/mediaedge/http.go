package mediaedge

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"html/template"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

//go:embed player/*
var playerFiles embed.FS

var playerPage = template.Must(template.ParseFS(playerFiles, "player/index.html"))

type httpServer struct {
	server         httpBackend
	http           *http.Server
	allowedOrigins map[string]struct{}
}

type httpBackend interface {
	HTTPConfig() Config
	SourceStatuses() []SourceStatus
	OpenSession(context.Context, string, SessionOffer) (SessionAnswer, error)
	CloseSession(string) bool
	StartRecording(context.Context, string, StartRecordingRequest) (RecordingManifest, error)
	Recordings() []RecordingManifest
	Recording(string) (RecordingManifest, bool)
	StopRecording(context.Context, string) (RecordingManifest, error)
	CaptureSnapshot(context.Context, string) (Snapshot, error)
	Snapshot(string) (Snapshot, bool)
	DeleteSnapshot(string) bool
}

func newHTTPServer(server httpBackend) *httpServer {
	config := server.HTTPConfig()
	allowedOrigins := make(map[string]struct{}, len(config.AllowedOrigins))
	for _, origin := range config.AllowedOrigins {
		allowedOrigins[origin] = struct{}{}
	}
	httpServer := &httpServer{server: server, allowedOrigins: allowedOrigins}
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
	path := strings.Trim(strings.TrimSpace(request.URL.Path), "/")
	parts := strings.Split(path, "/")
	if recordingHTTPRoute(parts) {
		if !requestFromLoopback(request) {
			writeError(writer, http.StatusForbidden, "media edge recording API is loopback-only")
			return
		}
		server.routeRecording(writer, request, parts)
		return
	}
	if snapshotHTTPRoute(parts) {
		if !requestFromLoopback(request) {
			writeError(writer, http.StatusForbidden, "media edge snapshot API is loopback-only")
			return
		}
		server.routeSnapshot(writer, request, parts)
		return
	}
	addVary(writer.Header(), "Origin")
	if request.Method == http.MethodOptions {
		server.handleCORSPreflight(writer, request, parts)
		return
	}
	if !server.applyCORS(writer, request) {
		return
	}
	switch {
	case request.Method == http.MethodGet && request.URL.Path == "/":
		server.serveSelectedPlayer(writer, request.URL.Query().Get("source"))
		return
	case request.Method == http.MethodGet && len(parts) == 2 && parts[0] == "sources":
		server.serveSelectedPlayer(writer, parts[1])
		return
	case request.Method == http.MethodGet && request.URL.Path == "/assets/player.css":
		server.servePlayerAsset(writer, "player/player.css", "text/css; charset=utf-8")
		return
	case request.Method == http.MethodGet && request.URL.Path == "/assets/player.js":
		server.servePlayerAsset(writer, "player/player.js", "text/javascript; charset=utf-8")
		return
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
	default:
		writeError(writer, http.StatusNotFound, "media edge endpoint was not found")
	}
}

func (server *httpServer) routeRecording(
	writer http.ResponseWriter,
	request *http.Request,
	parts []string,
) {
	switch {
	case request.Method == http.MethodPost && len(parts) == 5 &&
		parts[0] == "api" && parts[1] == "v1" && parts[2] == "sources" && parts[4] == "recordings":
		var input StartRecordingRequest
		if !decodeJSON(writer, request, &input) {
			return
		}
		recording, err := server.server.StartRecording(request.Context(), parts[3], input)
		if err != nil {
			writeError(writer, recordingHTTPStatus(err), err.Error())
			return
		}
		writeJSON(writer, http.StatusCreated, recording)
	case request.Method == http.MethodGet && len(parts) == 3 &&
		parts[0] == "api" && parts[1] == "v1" && parts[2] == "recordings":
		writeJSON(writer, http.StatusOK, struct {
			Recordings []RecordingManifest `json:"recordings"`
		}{Recordings: server.server.Recordings()})
	case request.Method == http.MethodGet && len(parts) == 4 &&
		parts[0] == "api" && parts[1] == "v1" && parts[2] == "recordings":
		recording, found := server.server.Recording(parts[3])
		if !found {
			writeError(writer, http.StatusNotFound, ErrRecordingNotFound.Error())
			return
		}
		writeJSON(writer, http.StatusOK, recording)
	case request.Method == http.MethodDelete && len(parts) == 4 &&
		parts[0] == "api" && parts[1] == "v1" && parts[2] == "recordings":
		recording, err := server.server.StopRecording(request.Context(), parts[3])
		if err != nil {
			writeError(writer, recordingHTTPStatus(err), err.Error())
			return
		}
		writeJSON(writer, http.StatusOK, recording)
	default:
		writeError(writer, http.StatusNotFound, "media edge endpoint was not found")
	}
}

func recordingHTTPRoute(parts []string) bool {
	return (len(parts) == 5 &&
		parts[0] == "api" && parts[1] == "v1" && parts[2] == "sources" && parts[4] == "recordings") ||
		(len(parts) == 3 &&
			parts[0] == "api" && parts[1] == "v1" && parts[2] == "recordings") ||
		(len(parts) == 4 &&
			parts[0] == "api" && parts[1] == "v1" && parts[2] == "recordings")
}

func (server *httpServer) routeSnapshot(
	writer http.ResponseWriter,
	request *http.Request,
	parts []string,
) {
	switch {
	case request.Method == http.MethodPost && len(parts) == 5 &&
		parts[0] == "api" && parts[1] == "v1" && parts[2] == "sources" && parts[4] == "snapshots":
		server.captureSnapshot(writer, request, parts[3])
	case request.Method == http.MethodGet && len(parts) == 5 &&
		parts[0] == "api" && parts[1] == "v1" && parts[2] == "snapshots" && parts[4] == "raw":
		server.readSnapshotRaw(writer, parts[3])
	case request.Method == http.MethodGet && len(parts) == 5 &&
		parts[0] == "api" && parts[1] == "v1" && parts[2] == "snapshots" && parts[4] == "jpeg":
		server.readSnapshotJPEG(writer, parts[3])
	case request.Method == http.MethodDelete && len(parts) == 4 &&
		parts[0] == "api" && parts[1] == "v1" && parts[2] == "snapshots":
		server.deleteSnapshot(writer, parts[3])
	default:
		writeError(writer, http.StatusNotFound, "media edge endpoint was not found")
	}
}

func snapshotHTTPRoute(parts []string) bool {
	return (len(parts) == 5 &&
		parts[0] == "api" && parts[1] == "v1" && parts[2] == "sources" && parts[4] == "snapshots") ||
		(len(parts) == 5 &&
			parts[0] == "api" && parts[1] == "v1" && parts[2] == "snapshots" &&
			(parts[4] == "raw" || parts[4] == "jpeg")) ||
		(len(parts) == 4 &&
			parts[0] == "api" && parts[1] == "v1" && parts[2] == "snapshots")
}

func (server *httpServer) serveSelectedPlayer(writer http.ResponseWriter, requested string) {
	sourceID := strings.TrimSpace(requested)
	if sourceID == "" {
		sourceID = server.server.HTTPConfig().Sources[0].ID
	}
	if !server.hasConfiguredSource(sourceID) {
		writeError(writer, http.StatusNotFound, "media source was not found")
		return
	}
	server.servePlayer(writer, sourceID)
}

func (server *httpServer) hasConfiguredSource(sourceID string) bool {
	for _, source := range server.server.HTTPConfig().Sources {
		if source.ID == sourceID {
			return true
		}
	}
	return false
}

func (server *httpServer) servePlayer(writer http.ResponseWriter, sourceID string) {
	writer.Header().Set("Content-Type", "text/html; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-store")
	setPlayerSecurityHeaders(writer.Header())
	writer.WriteHeader(http.StatusOK)
	_ = playerPage.ExecuteTemplate(writer, "index.html", struct {
		SourceID string
	}{SourceID: sourceID})
}

func (server *httpServer) servePlayerAsset(writer http.ResponseWriter, name string, contentType string) {
	content, err := playerFiles.ReadFile(name)
	if err != nil {
		writeError(writer, http.StatusNotFound, "media edge player asset was not found")
		return
	}
	writer.Header().Set("Content-Type", contentType)
	// Asset paths are intentionally unversioned; never let an Edge upgrade pair
	// a new session API with a stale cached player script.
	writer.Header().Set("Cache-Control", "no-store")
	setPlayerSecurityHeaders(writer.Header())
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(content)
}

func setPlayerSecurityHeaders(header http.Header) {
	header.Set("Content-Security-Policy",
		"default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; media-src blob:; "+
			"base-uri 'none'; form-action 'none'; frame-ancestors 'none'")
	header.Set("Referrer-Policy", "no-referrer")
	header.Set("X-Content-Type-Options", "nosniff")
}

func (server *httpServer) applyCORS(writer http.ResponseWriter, request *http.Request) bool {
	origin := strings.TrimSpace(request.Header.Get("Origin"))
	if origin == "" {
		return true
	}
	normalized, allowed := server.allowedOrigin(request, origin)
	if !allowed {
		writeError(writer, http.StatusForbidden, "browser origin is not allowed")
		return false
	}
	writer.Header().Set("Access-Control-Allow-Origin", normalized)
	return true
}

func (server *httpServer) handleCORSPreflight(
	writer http.ResponseWriter,
	request *http.Request,
	parts []string,
) {
	origin := strings.TrimSpace(request.Header.Get("Origin"))
	if origin == "" {
		writeError(writer, http.StatusBadRequest, "CORS preflight requires Origin")
		return
	}
	normalized, allowed := server.allowedOrigin(request, origin)
	if !allowed {
		writeError(writer, http.StatusForbidden, "browser origin is not allowed")
		return
	}
	method := strings.ToUpper(strings.TrimSpace(request.Header.Get("Access-Control-Request-Method")))
	if method == "" || !publicRouteAllowsMethod(request.URL.Path, parts, method) {
		writeError(writer, http.StatusMethodNotAllowed, "CORS method is not allowed for this endpoint")
		return
	}
	requestedHeaders := request.Header.Values("Access-Control-Request-Headers")
	allowContentType := false
	for _, value := range requestedHeaders {
		for _, header := range strings.Split(value, ",") {
			header = strings.TrimSpace(header)
			if header == "" {
				continue
			}
			if !strings.EqualFold(header, "Content-Type") {
				writeError(writer, http.StatusForbidden, "CORS request header is not allowed")
				return
			}
			allowContentType = true
		}
	}
	writer.Header().Set("Access-Control-Allow-Origin", normalized)
	writer.Header().Set("Access-Control-Allow-Methods", method)
	if allowContentType {
		writer.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	}
	writer.Header().Set("Access-Control-Max-Age", "600")
	addVary(writer.Header(), "Access-Control-Request-Method")
	addVary(writer.Header(), "Access-Control-Request-Headers")
	writer.WriteHeader(http.StatusNoContent)
}

func (server *httpServer) allowedOrigin(request *http.Request, value string) (string, bool) {
	origin, err := normalizeHTTPOrigin(value)
	if err != nil {
		return "", false
	}
	if _, allowed := server.allowedOrigins[origin]; allowed {
		return origin, true
	}
	scheme := "http"
	if request.TLS != nil {
		scheme = "https"
	}
	requestOrigin, err := normalizeHTTPOrigin(scheme + "://" + request.Host)
	return origin, err == nil && origin == requestOrigin
}

func publicRouteAllowsMethod(path string, parts []string, method string) bool {
	switch {
	case path == "/" || path == "/assets/player.css" || path == "/assets/player.js":
		return method == http.MethodGet
	case strings.Trim(strings.TrimSpace(path), "/") == "healthz":
		return method == http.MethodGet
	case len(parts) == 5 &&
		parts[0] == "api" && parts[1] == "v1" && parts[2] == "sources" && parts[4] == "sessions":
		return method == http.MethodPost
	case len(parts) == 4 &&
		parts[0] == "api" && parts[1] == "v1" && parts[2] == "sessions":
		return method == http.MethodDelete
	default:
		return false
	}
}

func addVary(header http.Header, value string) {
	for _, current := range header.Values("Vary") {
		for _, item := range strings.Split(current, ",") {
			if strings.EqualFold(strings.TrimSpace(item), value) {
				return
			}
		}
	}
	header.Add("Vary", value)
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
