package mediamtx

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
)

func TestClientReadsPathsAndPatchesRecording(t *testing.T) {
	var patched bool
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/v3/paths/list":
			_ = json.NewEncoder(writer).Encode(map[string]any{"items": []any{map[string]any{
				"name": "front", "available": true, "online": true, "inboundBytes": 123,
				"inboundFramesInError": 0, "readers": []any{},
				"tracks2": []any{map[string]any{"codec": "H264", "codecProps": map[string]any{"width": 640, "height": 360}}},
			}}})
		case "/v3/config/paths/patch/front":
			if request.Method != http.MethodPatch {
				t.Fatalf("patch method = %s", request.Method)
			}
			var body map[string]bool
			_ = json.NewDecoder(request.Body).Decode(&body)
			patched = body["record"]
			_ = json.NewEncoder(writer).Encode(map[string]string{"status": "ok"})
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	client := newTestClient(t, server.URL)
	paths, err := client.Paths(context.Background())
	if err != nil {
		t.Fatalf("list paths: %v", err)
	}
	if len(paths) != 1 || paths[0].Name != "front" || paths[0].InboundBytes != 123 ||
		len(paths[0].Tracks) != 1 || paths[0].Tracks[0].Codec != "H264" {
		t.Fatalf("paths = %+v", paths)
	}
	if err := client.SetRecording(context.Background(), "front", true); err != nil {
		t.Fatalf("enable recording: %v", err)
	}
	if !patched {
		t.Fatal("recording patch did not enable record")
	}
}

func TestClientOpensAndClosesWHEP(t *testing.T) {
	var deleted bool
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodPost && request.URL.Path == "/front/whep":
			if request.Header.Get("Content-Type") != "application/sdp" {
				t.Fatalf("content type = %q", request.Header.Get("Content-Type"))
			}
			if request.URL.Query().Get("xgcSession") != "xgc-session-1" {
				t.Fatalf("XGC WHEP session token = %q", request.URL.Query().Get("xgcSession"))
			}
			writer.Header().Set("Content-Type", "application/sdp")
			writer.Header().Set("Location", "/front/whep/01234567-89ab-cdef-0123-456789abcdef?xgcSession=xgc-session-1")
			writer.WriteHeader(http.StatusCreated)
			_, _ = writer.Write([]byte("v=0\r\na=recvonly\r\n"))
		case request.Method == http.MethodDelete && strings.HasPrefix(request.URL.Path, "/front/whep/"):
			deleted = true
			writer.WriteHeader(http.StatusOK)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	client := newTestClient(t, server.URL)
	session, err := client.OpenWHEP(context.Background(), "front", "v=0\r\na=sendrecv\r\n", "xgc-session-1")
	if err != nil {
		t.Fatalf("open WHEP: %v", err)
	}
	if session.AnswerSDP == "" || !strings.HasSuffix(session.Location.Path, "456789abcdef") {
		t.Fatalf("WHEP session = %+v", session)
	}
	closed, err := client.CloseWHEP(context.Background(), session.Location)
	if err != nil || !closed || !deleted {
		t.Fatalf("close WHEP = closed=%v deleted=%v err=%v", closed, deleted, err)
	}
}

func TestClientRejectsEscapingWHEPLocation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/sdp")
		writer.Header().Set("Location", "http://attacker.example/front/whep/session")
		writer.WriteHeader(http.StatusCreated)
		_, _ = writer.Write([]byte("v=0\r\n"))
	}))
	defer server.Close()
	client := newTestClient(t, server.URL)
	if _, err := client.OpenWHEP(context.Background(), "front", "v=0\r\n", "xgc-session-1"); err == nil ||
		!strings.Contains(err.Error(), "outside") {
		t.Fatalf("escaping WHEP Location error = %v", err)
	}
	escaping, _ := url.Parse("http://attacker.example/front/whep/session")
	if _, err := client.CloseWHEP(context.Background(), escaping); err == nil {
		t.Fatal("close accepted escaping WHEP Location")
	}
}

func TestNewClientRequiresLoopback(t *testing.T) {
	for _, value := range []string{"https://127.0.0.1:9997", "http://192.168.1.20:9997", "http://user@127.0.0.1:9997"} {
		if _, err := NewClient(value, "http://127.0.0.1:8889"); err == nil {
			t.Fatalf("NewClient accepted %q", value)
		}
	}
}

func newTestClient(t *testing.T, base string) *Client {
	t.Helper()
	client, err := NewClient(base, base)
	if err != nil {
		t.Fatalf("create MediaMTX client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestClientBoundedInventoryAndNativeKickUUID(t *testing.T) {
	const apiID = "01234567-89ab-cdef-0123-456789abcdef"
	incomplete := false
	kicks := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v3/webrtcsessions/list":
			if r.Method != http.MethodGet || r.URL.Query().Get("itemsPerPage") != "1024" {
				t.Error("session inventory did not request a finite full page")
			}
			count, pages := 1, 1
			if incomplete {
				count, pages = 1025, 2
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{"id": apiID, "path": "front", "state": "read", "query": "xgcSession=token"}}, "itemCount": count, "pageCount": pages})
		case "/v3/webrtcsessions/kick/" + apiID:
			if r.Method != http.MethodPost {
				t.Error("native kick method must be POST")
			}
			kicks++
			if kicks > 1 {
				w.WriteHeader(404)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := newTestClient(t, server.URL)
	items, err := client.WebRTCSessions(t.Context())
	if err != nil || len(items) != 1 || items[0].ID != apiID {
		t.Fatalf("inventory: %v %v", items, err)
	}
	if err := client.KickWebRTCSession(t.Context(), items[0].ID); err != nil {
		t.Fatal(err)
	}
	if err := client.KickWebRTCSession(t.Context(), items[0].ID); err != nil {
		t.Fatalf("already absent UUID: %v", err)
	}
	incomplete = true
	if _, err := client.WebRTCSessions(t.Context()); err == nil {
		t.Fatal("partial native inventory was treated as authoritative absence")
	}
}

func TestNativeWHEPCreationLostResponseIsNotReplayed(t *testing.T) {
	var posts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}})
			return
		}
		posts.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		connection, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		_ = connection.Close()
	}))
	defer server.Close()
	client := newTestClient(t, server.URL)
	// First create a reusable connection, then lose the mutation response.
	if _, err := client.Paths(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := client.OpenWHEP(t.Context(), "front", "v=0\r\n", "one-token"); err == nil {
		t.Fatal("lost response reported success")
	}
	if posts.Load() != 1 {
		t.Fatalf("native transport replayed creation %d times", posts.Load())
	}
}
