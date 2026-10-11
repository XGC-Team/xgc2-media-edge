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

func runtimeFixtureConfig(t *testing.T) Config {
	t.Helper()
	config, err := (Config{ControlAddress: "127.0.0.1:0", Sources: []SourceConfig{{ID: "camera", RTPListenAddress: "127.0.0.1:5004", ControlSocket: "/run/media-test/source.sock"}}}).normalized()
	if err != nil {
		t.Fatal(err)
	}
	return config
}

func TestRuntimeLimitsExecuteHostAndClientSettings(t *testing.T) {
	config := runtimeFixtureConfig(t)
	config.HostMaxInFlight = 2
	config.HostMaxConnections = 3
	config.ClientMaxConnections = 1
	config.MaxResponseBytes = 4096
	config.ShutdownTimeout = 2 * time.Second
	control := newCaptureControl(t)
	defer control.close()
	config.Sources[0].ControlSocket = control.socket
	server := newMediaMTXServer(config, MediaMTXSettings{}, newFakeMediaMTXControl("camera"), newFakeMediaMTXProcess())
	defer server.cancelLifecycle()
	options := server.config.hostOptions(true)
	if options.MaxInFlight != 2 || options.MaxConnections != 3 || options.MaxResponseBytes != 4096 || options.ShutdownTimeout.Milliseconds() != 2000 {
		t.Fatalf("ignored host limits: %+v", options)
	}
	if server.config.MaxOperations != 2 || server.config.MaxCaptureBytes != 4096 {
		t.Fatal("product domain admission ignored transport ceiling")
	}
	rpc := server.sources["camera"].controller().(*cameraPeer).control
	defer rpc.close()
	client, err := rpc.transport(t.Context())
	if err == nil || client != nil {
		t.Fatal("client call without finite caller deadline should fail")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	client, err = rpc.transport(ctx)
	if err != nil || client.Status().ConnectionLimit != 1 || client.Status().InFlightLimit != 2 {
		t.Fatalf("source client pool did not execute limits: %v", err)
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
	for _, path := range []string{private, filepath.Join(private, "control"), filepath.Join(private, "mediamtx"), filepath.Join(private, "pipelines")} {
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

func TestBrowserHostExecutesResponseByteLimitOnNativeTCP(t *testing.T) {
	config := runtimeFixtureConfig(t)
	config.MaxResponseBytes = 1024
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
			t.Fatal("public ServeEdge silently ignored response byte limit")
		}
	}
}
