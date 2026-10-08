package mediaedge

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func policyFixtureConfig(t *testing.T) Config {
	t.Helper()
	config, err := (Config{ControlAddress: "127.0.0.1:0", Sources: []SourceConfig{{ID: "camera", RTPListenAddress: "127.0.0.1:5004", ControlSocket: "/run/media-test/source.sock"}}}).normalized()
	if err != nil {
		t.Fatal(err)
	}
	return config
}

func TestRuntimePolicySnapshotExecutesHostAndClientSettings(t *testing.T) {
	config := policyFixtureConfig(t)
	environment := []string{"XGC2_XRPC_HOST_MAX_IN_FLIGHT=2", "XGC2_XRPC_HOST_MAX_CONNECTIONS=3", "XGC2_XRPC_CLIENT_MAX_CONNECTIONS=1", "XGC2_XRPC_MAX_RESPONSE_BYTES=4096", "XGC2_XRPC_SHUTDOWN_TIMEOUT_MS=2000"}
	policy, err := ResolveRuntimePolicy(environment, config)
	if err != nil {
		t.Fatal(err)
	}
	environment[0] = "XGC2_XRPC_HOST_MAX_IN_FLIGHT=99"
	config.RuntimePolicy = policy
	options, err := config.hostOptions(true)
	if err != nil {
		t.Fatal(err)
	}
	if options.MaxInFlight != 2 || options.MaxConnections != 3 || options.MaxResponseBytes != 4096 || options.ShutdownTimeout.Milliseconds() != 2000 {
		t.Fatalf("ignored host policy: %+v", options)
	}
	server := newMediaMTXServer(config, MediaMTXSettings{}, newFakeMediaMTXControl("camera"), newFakeMediaMTXProcess())
	if server.config.MaxOperations != 2 || server.config.MaxCaptureBytes != 4096 {
		t.Fatal("product domain admission ignored transport ceiling")
	}
	control := newCaptureControl(t)
	defer control.close()
	rpc := &cameraControl{socket: control.socket, sourceID: "camera", policy: policy}
	defer rpc.close()
	client, err := rpc.transport(t.Context())
	if err == nil || client != nil {
		t.Fatal("client call without finite caller deadline should fail")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err = rpc.transport(ctx)
	if err != nil || client.Status().ConnectionLimit != 1 || client.Status().InFlightLimit != 2 {
		t.Fatalf("source client pool did not execute policy: %v", err)
	}
	// Unsupported roles and malformed values are startup errors, not settings
	// parsed successfully and then silently ignored by this composition.
	for _, value := range []string{"XGC2_XRPC_LOG_LEVEL=debug", "XGC2_XRPC_CLIENT_MAX_REFERENCES=2", "XGC2_XRPC_GRPC_MAX_STREAMS_PER_CONNECTION=1", "XGC2_XRPC_HOST_MAX_IN_FLIGHT=03", "XGC2_XRPC_CLIENT_MAX_CONNECTIONS=3", "XGC2_XRPC_HOST_MAX_IN_FLIGHT=17"} {
		if _, err := ResolveRuntimePolicy([]string{value}, config); err == nil {
			t.Fatalf("unexecuted or invalid setting accepted: %s", value)
		}
	}
}

func TestGrantedRuntimePreparationRejectsExistingForeignModeAndSymlinks(t *testing.T) {
	root, err := os.MkdirTemp("", "media-grant-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	private := filepath.Join(root, "owned")
	script := filepath.Join("..", "..", "scripts", "prepare_runtime.py")
	arguments := []string{script, "--runtime-root", private, "--uid", strconv.Itoa(os.Geteuid()), "--gid", strconv.Itoa(os.Getegid())}
	cmd := exec.Command("python3", arguments...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("runtime grant: %v %s", err, output)
	}
	for _, path := range []string{private, filepath.Join(private, "control"), filepath.Join(private, "mediamtx")} {
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != 0700 {
			t.Fatalf("grant mode at %s: %v", path, err)
		}
	}
	if err = os.Chmod(filepath.Join(private, "control"), 0755); err != nil {
		t.Fatal(err)
	}
	if output, err = exec.Command("python3", arguments...).CombinedOutput(); err == nil || !strings.Contains(string(output), "not already granted") {
		t.Fatal("preparation silently chmodded a foreign/existing runtime path")
	}
	link := filepath.Join(root, "link")
	if err = os.Symlink(private, link); err != nil {
		t.Fatal(err)
	}
	arguments[2] = filepath.Join(link, "new")
	if _, err = exec.Command("python3", arguments...).CombinedOutput(); err == nil {
		t.Fatal("runtime grant followed a symlink ancestor")
	}
}

func TestBrowserHostExecutesResponseBytePolicyOnNativeTCP(t *testing.T) {
	config := policyFixtureConfig(t)
	var err error
	config.RuntimePolicy, err = ResolveRuntimePolicy([]string{"XGC2_XRPC_MAX_RESPONSE_BYTES=1024"}, config)
	if err != nil {
		t.Fatal(err)
	}
	edge := newMediaMTXServer(config, MediaMTXSettings{}, newFakeMediaMTXControl("camera"), newFakeMediaMTXProcess())
	defer edge.cancelLifecycle()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	host := newHTTPServer(edge)
	if err := host.start(listener); err != nil {
		t.Fatal(err)
	}
	defer host.close()
	transport := &http.Transport{Proxy: nil}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: time.Second}
	base := "http://" + listener.Addr().String()
	reply, err := client.Get(base + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	_, readErr := io.ReadAll(reply.Body)
	reply.Body.Close()
	if reply.StatusCode != 200 || readErr != nil {
		t.Fatal("bounded health reply failed")
	}
	reply, err = client.Get(base + "/assets/player.js")
	if err == nil {
		body, bodyErr := io.ReadAll(reply.Body)
		reply.Body.Close()
		if len(body) > 1024 || (reply.StatusCode == 200 && bodyErr == nil) {
			t.Fatal("public ServeEdge silently ignored MAX_RESPONSE_BYTES")
		}
	}
}
