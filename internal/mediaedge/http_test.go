package mediaedge

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestExplicitWildcardHTTPListenerServesDirectSurface(t *testing.T) {
	capture := newCaptureControl(t)
	defer capture.close()
	rtpAddress := availableLoopbackRTPAddress(t)
	capture.setRTPDestination(t, rtpAddress)
	server, err := New(Config{
		ControlAddress: "0.0.0.0:0",
		Sources: []SourceConfig{{
			ID: "camera", RTPListenAddress: rtpAddress, ControlSocket: capture.socket,
		}},
	})
	if err != nil {
		t.Fatalf("create wildcard media edge: %v", err)
	}
	defer server.Close()
	if err := server.Start(); err != nil {
		t.Fatalf("start wildcard media edge: %v", err)
	}
	_, port, err := net.SplitHostPort(server.ControlAddress())
	if err != nil {
		t.Fatalf("split wildcard HTTP address: %v", err)
	}
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get("http://127.0.0.1:" + port + "/")
	if err != nil {
		t.Fatalf("open direct player through wildcard listener: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK ||
		!strings.HasPrefix(response.Header.Get("Content-Type"), "text/html") {
		t.Fatalf("wildcard direct player returned %s with %q",
			response.Status, response.Header.Get("Content-Type"))
	}
}

func TestRemoteHTTPPublishesOnlyPlayerHealthAndSessions(t *testing.T) {
	handler := newHTTPTestServer(t, "camera", nil)

	root := performHTTPRequest(handler, http.MethodGet, "/", "", "192.0.2.44:42000", nil)
	if root.Code != http.StatusOK {
		t.Fatalf("GET / returned %d: %s", root.Code, root.Body.String())
	}
	if contentType := root.Header().Get("Content-Type"); !strings.HasPrefix(contentType, "text/html") {
		t.Fatalf("GET / content type = %q", contentType)
	}
	if body := root.Body.String(); !strings.Contains(body, `data-source-id="camera"`) ||
		!strings.Contains(body, "/assets/player.js") {
		t.Fatalf("GET / did not render the configured player: %s", body)
	}

	script := performHTTPRequest(handler, http.MethodGet, "/assets/player.js", "", "192.0.2.44:42000", nil)
	if script.Code != http.StatusOK ||
		script.Header().Get("Cache-Control") != "no-store" ||
		!strings.Contains(script.Body.String(), "new RTCPeerConnection()") ||
		!strings.Contains(script.Body.String(), `direction: "recvonly"`) ||
		!strings.Contains(script.Body.String(), `method: "DELETE"`) {
		t.Fatalf("embedded player script is incomplete: %d %s", script.Code, script.Body.String())
	}

	health := performHTTPRequest(handler, http.MethodGet, "/healthz", "", "192.0.2.44:42000", nil)
	if health.Code != http.StatusOK {
		t.Fatalf("remote health returned %d: %s", health.Code, health.Body.String())
	}

	session := performHTTPRequest(
		handler,
		http.MethodPost,
		"/api/v1/sources/camera/sessions",
		"{}",
		"192.0.2.44:42000",
		map[string]string{"Content-Type": "application/json"},
	)
	if session.Code != http.StatusBadRequest ||
		!strings.Contains(session.Body.String(), "offer SDP is required") {
		t.Fatalf("remote session POST did not reach signaling: %d %s", session.Code, session.Body.String())
	}
	closeSession := performHTTPRequest(
		handler,
		http.MethodDelete,
		"/api/v1/sessions/unknown",
		"",
		"192.0.2.44:42000",
		nil,
	)
	if closeSession.Code != http.StatusNotFound {
		t.Fatalf("remote session DELETE did not reach signaling: %d %s", closeSession.Code, closeSession.Body.String())
	}

	snapshot := performHTTPRequest(
		handler,
		http.MethodPost,
		"/api/v1/sources/camera/snapshots",
		"{}",
		"192.0.2.44:42000",
		map[string]string{"Content-Type": "application/json"},
	)
	if snapshot.Code != http.StatusForbidden ||
		!strings.Contains(snapshot.Body.String(), "loopback-only") {
		t.Fatalf("remote snapshot returned %d: %s", snapshot.Code, snapshot.Body.String())
	}

	localSnapshot := performHTTPRequest(
		handler,
		http.MethodPost,
		"/api/v1/sources/unknown/snapshots",
		"{}",
		"127.0.0.1:42000",
		map[string]string{"Content-Type": "application/json"},
	)
	if localSnapshot.Code != http.StatusNotFound {
		t.Fatalf("loopback snapshot did not reach the local route: %d %s", localSnapshot.Code, localSnapshot.Body.String())
	}

	unknown := performHTTPRequest(handler, http.MethodGet, "/api/v1/streams", "", "192.0.2.44:42000", nil)
	if unknown.Code != http.StatusNotFound {
		t.Fatalf("unexpected discovery endpoint status = %d", unknown.Code)
	}
}

func TestPlayerEscapesServerInjectedSourceID(t *testing.T) {
	// Construct the renderer directly so this defense-in-depth test can exercise
	// an unsafe value that normal SourceConfig validation rejects.
	handler := newHTTPServer(&Server{config: Config{
		Sources: []SourceConfig{{ID: `camera" onload="alert(1)`}},
	}})
	response := performHTTPRequest(handler, http.MethodGet, "/", "", "192.0.2.44:42000", nil)
	if response.Code != http.StatusOK {
		t.Fatalf("GET / returned %d: %s", response.Code, response.Body.String())
	}
	body := response.Body.String()
	if strings.Contains(body, `data-source-id="camera" onload="alert(1)"`) {
		t.Fatalf("source ID escaped its data attribute: %s", body)
	}
	if !strings.Contains(body, "camera&#34; onload=&#34;alert(1)") {
		t.Fatalf("source ID was not safely injected by html/template: %s", body)
	}
}

func TestCORSUsesExactOriginsAndMinimalPreflight(t *testing.T) {
	handler := newHTTPTestServer(t, "camera", []string{"https://station.example:8443"})
	headers := map[string]string{
		"Origin":                         "https://station.example:8443",
		"Access-Control-Request-Method":  http.MethodPost,
		"Access-Control-Request-Headers": "content-type",
	}
	preflight := performHTTPRequest(
		handler,
		http.MethodOptions,
		"/api/v1/sources/camera/sessions",
		"",
		"192.0.2.44:42000",
		headers,
	)
	if preflight.Code != http.StatusNoContent {
		t.Fatalf("allowed preflight returned %d: %s", preflight.Code, preflight.Body.String())
	}
	if got := preflight.Header().Get("Access-Control-Allow-Origin"); got != headers["Origin"] {
		t.Fatalf("allow origin = %q, want %q", got, headers["Origin"])
	}
	if got := preflight.Header().Get("Access-Control-Allow-Methods"); got != http.MethodPost {
		t.Fatalf("allow methods = %q", got)
	}
	if got := preflight.Header().Get("Access-Control-Allow-Headers"); got != "Content-Type" {
		t.Fatalf("allow headers = %q", got)
	}
	if got := preflight.Header().Get("Access-Control-Allow-Credentials"); got != "" {
		t.Fatalf("credentials were unexpectedly allowed: %q", got)
	}

	health := performHTTPRequest(
		handler,
		http.MethodGet,
		"/healthz",
		"",
		"192.0.2.44:42000",
		map[string]string{"Origin": headers["Origin"]},
	)
	if health.Code != http.StatusOK ||
		health.Header().Get("Access-Control-Allow-Origin") != headers["Origin"] {
		t.Fatalf("allowed actual CORS request returned %d with headers %v", health.Code, health.Header())
	}

	denied := performHTTPRequest(
		handler,
		http.MethodOptions,
		"/api/v1/sources/camera/sessions",
		"",
		"192.0.2.44:42000",
		map[string]string{
			"Origin":                        "https://other.example:8443",
			"Access-Control-Request-Method": http.MethodPost,
		},
	)
	if denied.Code != http.StatusForbidden ||
		denied.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatalf("disallowed origin returned %d with headers %v", denied.Code, denied.Header())
	}

	excessHeader := performHTTPRequest(
		handler,
		http.MethodOptions,
		"/api/v1/sources/camera/sessions",
		"",
		"192.0.2.44:42000",
		map[string]string{
			"Origin":                         headers["Origin"],
			"Access-Control-Request-Method":  http.MethodPost,
			"Access-Control-Request-Headers": "authorization",
		},
	)
	if excessHeader.Code != http.StatusForbidden {
		t.Fatalf("unexpected CORS header returned %d", excessHeader.Code)
	}

	remoteSnapshot := performHTTPRequest(
		handler,
		http.MethodOptions,
		"/api/v1/sources/camera/snapshots",
		"",
		"192.0.2.44:42000",
		headers,
	)
	if remoteSnapshot.Code != http.StatusForbidden {
		t.Fatalf("remote snapshot preflight returned %d", remoteSnapshot.Code)
	}
}

func TestStandalonePlayerSameOriginNeedsNoAllowlistEntry(t *testing.T) {
	handler := newHTTPTestServer(t, "camera", nil)
	response := performHTTPRequest(
		handler,
		http.MethodOptions,
		"/api/v1/sources/camera/sessions",
		"",
		"192.0.2.44:42000",
		map[string]string{
			"Origin":                         "http://robot.example:18090",
			"Access-Control-Request-Method":  http.MethodPost,
			"Access-Control-Request-Headers": "Content-Type",
		},
	)
	if response.Code != http.StatusNoContent ||
		response.Header().Get("Access-Control-Allow-Origin") != "http://robot.example:18090" {
		t.Fatalf("same-origin preflight returned %d with headers %v: %s",
			response.Code, response.Header(), response.Body.String())
	}
}

func newHTTPTestServer(t *testing.T, sourceID string, allowedOrigins []string) *httpServer {
	t.Helper()
	server, err := New(Config{
		ControlAddress: "127.0.0.1:0",
		AllowedOrigins: allowedOrigins,
		Sources: []SourceConfig{{
			ID: sourceID, RTPListenAddress: "127.0.0.1:5004",
			ControlSocket: "/tmp/xgc-media-edge-http-test.sock",
		}},
	})
	if err != nil {
		t.Fatalf("create HTTP test media edge: %v", err)
	}
	return newHTTPServer(server)
}

func performHTTPRequest(
	handler *httpServer,
	method string,
	target string,
	body string,
	remoteAddress string,
	headers map[string]string,
) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, target, strings.NewReader(body))
	request.RemoteAddr = remoteAddress
	request.Host = "robot.example:18090"
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	recorder := httptest.NewRecorder()
	handler.route(recorder, request)
	return recorder
}
