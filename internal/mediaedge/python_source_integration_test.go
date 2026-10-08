package mediaedge

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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

	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/httpx"
)

func TestRealPythonSourceControlAndEdgeCapture(t *testing.T) {
	if os.Getenv("XGC_MEDIA_SOURCE_FIXTURE") != "1" {
		t.Skip("set XGC_MEDIA_SOURCE_FIXTURE=1 for the real Python product boundary")
	}
	directory, err := os.MkdirTemp("", "media-xlang-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	socket := filepath.Join(directory, "python.sock")
	address := availableLoopbackRTPAddress(t)
	_, port, _ := net.SplitHostPort(address)
	pythonPath := os.Getenv("XGC_MEDIA_SOURCE_PYTHONPATH")
	if pythonPath == "" {
		pythonPath = "/tmp/xrpc-python-deps" + string(os.PathListSeparator) + filepath.Join("..", "..", "..", "xrpc", "python") + string(os.PathListSeparator) + filepath.Join("..", "..", "..", "..", "ros2", "driver", "ros_image_rtp_adapter")
		parts := strings.Split(pythonPath, string(os.PathListSeparator))
		for index, path := range parts {
			parts[index], _ = filepath.Abs(path)
		}
		pythonPath = strings.Join(parts, string(os.PathListSeparator))
	}
	cmd := exec.Command("python3", "-u", filepath.Join("testdata", "python_source_fixture.py"), socket, port)
	cmd.Env = append(os.Environ(), "PYTHONPATH="+pythonPath)
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() {
		input.Close()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Python fixture exit: %v: %s", err, stderr.String())
			}
		case <-time.After(3 * time.Second):
			_ = cmd.Process.Kill()
			<-done
			t.Error("Python fixture did not stop")
		}
	})
	lines := make(chan string, 2)
	go func() {
		scanner := bufio.NewScanner(output)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		close(lines)
	}()
	ready := func() string {
		select {
		case line := <-lines:
			var value struct {
				Instance string `json:"instance_id"`
			}
			if err = json.Unmarshal([]byte(line), &value); err != nil || value.Instance == "" {
				t.Fatalf("Python readiness %q err=%v logs=%s", line, err, stderr.String())
			}
			return value.Instance
		case <-time.After(5 * time.Second):
			t.Fatal("Python adapter did not become ready")
			return ""
		}
	}
	first := ready()
	config, err := (Config{ControlAddress: "127.0.0.1:0", RPCSocket: filepath.Join(directory, "edge", "control.sock"), SessionGracePeriod: time.Hour, Sources: []SourceConfig{{ID: "python-camera", RTPListenAddress: address, ControlSocket: socket}}}).normalized()
	if err != nil {
		t.Fatal(err)
	}
	config.RuntimePolicy, err = ResolveRuntimePolicy([]string{"XGC2_XRPC_HOST_MAX_IN_FLIGHT=4", "XGC2_XRPC_CLIENT_MAX_CONNECTIONS=1"}, config)
	if err != nil {
		t.Fatal(err)
	}
	edge := newMediaMTXServer(config, MediaMTXSettings{}, newFakeMediaMTXControl("python-camera"), newFakeMediaMTXProcess())
	if err = edge.Start(); err != nil {
		t.Fatal(err)
	}
	restarted := false
	defer func() {
		err := edge.Close()
		if restarted && err == nil {
			t.Error("stale-source close pretended native stop succeeded")
		}
		if !restarted && err != nil {
			t.Errorf("edge close: %v", err)
		}
		edge.sources["python-camera"].cameraControl().close()
	}()
	// Discover the real edge host, then bind its fresh process incarnation.
	target, _ := os.Hostname()
	ref := xrpc.ServiceRef{TargetID: target, Service: "media-edge", APIVersion: "v1", Profile: xrpc.HTTP, Endpoint: xrpc.Endpoint{Kind: "unix", Address: config.RPCSocket}}
	client, err := httpx.New(httpx.Config{LocalTargetID: target, Service: ref, MaxResponseBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	data, status, _, err := client.Do(ctx, http.MethodGet, "/v1/describe", "fixture:discover", "", nil)
	client.Close()
	if err != nil || status != 200 {
		t.Fatalf("edge discovery status=%d err=%v data=%s", status, err, data)
	}
	var description struct {
		Service xrpc.ServiceRef `json:"service_ref"`
	}
	if err = json.Unmarshal(data, &description); err != nil {
		t.Fatal(err)
	}
	client, err = httpx.New(httpx.Config{LocalTargetID: target, Service: description.Service, MaxResponseBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	control := edge.sources["python-camera"].cameraControl()
	data, status, _, err = client.Do(ctx, http.MethodGet, "/v1/media/sources/python-camera/ref", "fixture:source-ref", "", nil)
	if err != nil || status != http.StatusOK {
		t.Fatalf("bound source reference HTTP %d err=%v data=%s", status, err, data)
	}
	var sourceReference struct {
		SourceID string          `json:"source_id"`
		Service  xrpc.ServiceRef `json:"service_ref"`
	}
	if err = json.Unmarshal(data, &sourceReference); err != nil {
		t.Fatal(err)
	}
	if sourceReference.SourceID != "python-camera" || sourceReference.Service.InstanceID != first || sourceReference.Service.Endpoint.Address != socket || sourceReference.Service.Service != "camera-source" {
		t.Fatalf("edge invented a source reference: %+v", sourceReference)
	}
	// The calibration owner can use the returned source-owned reference directly;
	// no inferred socket, environment bridge, or Edge RTP-only config projection.
	sourceClient, err := httpx.New(httpx.Config{LocalTargetID: target, Service: sourceReference.Service, MaxResponseBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer sourceClient.Close()
	data, status, _, err = sourceClient.Do(ctx, http.MethodGet, "/v1/media/sources/python-camera/config", "fixture:direct-source-config", "", nil)
	if err != nil || status != http.StatusOK {
		t.Fatalf("source-ref direct SDK configuration HTTP %d err=%v data=%s", status, err, data)
	}
	configuration, _, _, err := control.call(ctx, sourceControlRequest{Operation: "config"})
	if err != nil || configuration.ManagedSourceID != "python-camera" || configuration.DesiredRevision != 1 || configuration.AppliedRevision != 1 || configuration.Persistence != "ephemeral" || configuration.PersistedRevision != nil || configuration.Applied.RTPPort != configuration.Desired.RTPPort {
		t.Fatalf("Python configuration revision/receipt: %+v %v", configuration, err)
	}
	revision, persist := uint64(1), false
	patch := sourceControlRequest{Operation: "configure", ExpectedRevision: &revision, Persist: &persist, Config: &sourceRuntimeConfiguration{Bitrate: 2100000}}
	configuration, _, _, err = control.call(ctx, patch)
	if err != nil || configuration.DesiredRevision != 2 || configuration.AppliedRevision != 2 || configuration.Applied.Bitrate != 2100000 {
		t.Fatalf("Python PATCH applied revision: %+v %v", configuration, err)
	}
	if _, _, _, err = control.call(ctx, patch); err == nil {
		t.Fatal("stale expected_revision accepted")
	}
	revision, persist = 2, true
	if _, _, _, err = control.call(ctx, patch); err == nil {
		t.Fatal("unsupported persistence was reported applied")
	}
	persist = false
	for index, include := range []bool{true, false} {
		payload := fmt.Sprintf(`{"includeRgb":%t,"requestKeyframe":false,"requireFresh":true}`, include)
		reply, err := client.DoStream(ctx, http.MethodPost, "/v1/media/sources/python-camera/capture", "fixture:capture:"+strconv.Itoa(index), "application/json", []byte(payload))
		if err != nil {
			t.Fatal(err)
		}
		if reply.StatusCode != 200 {
			body, _ := io.ReadAll(reply.Body)
			reply.Body.Close()
			t.Fatalf("capture status=%d: %s", reply.StatusCode, body)
		}
		metadata, jpeg, rgb, err := readCameraMultipart(reply.Header.Get("Content-Type"), reply.Body, include)
		reply.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if metadata.FrameID != "python_optical" || metadata.Sequence != uint64(8+index) || metadata.TimestampNanoseconds != 123456789 || metadata.TimestampClockDomain != "device" || metadata.CalibrationState != "unavailable" || len(metadata.CameraMatrix) != 0 || len(jpeg) == 0 {
			t.Fatalf("same-frame source metadata lost: %+v", metadata)
		}
		if include {
			if len(rgb) != 16*16*3 {
				t.Fatal("RGB size mismatch")
			}
			for position, value := range rgb {
				if value != byte(position%251) {
					t.Fatal("RGB bytes changed")
				}
			}
		} else if len(rgb) != 0 {
			t.Fatal("JPEG-only capture leaked RGB")
		}
		_, deleteStatus, _, deleteErr := client.Do(ctx, http.MethodDelete, "/v1/media/snapshots/"+metadata.SnapshotID, "fixture:delete:"+strconv.Itoa(index), "", nil)
		if deleteErr != nil || deleteStatus != http.StatusNoContent {
			t.Fatalf("instance-bound retention DELETE: %d %v", deleteStatus, deleteErr)
		}
		if _, found := edge.Snapshot(metadata.SnapshotID); found {
			t.Fatal("deleted capture remained retained")
		}
	}
	nativeStatus, _, _, err := control.call(ctx, sourceControlRequest{Operation: "status"})
	if err != nil || !nativeStatus.AppliedActive || nativeStatus.ConfigurationRevision != 2 || nativeStatus.ManagedSourceID != "python-camera" {
		t.Fatalf("Python native status: %+v %v", nativeStatus, err)
	}
	if _, _, _, err = control.call(ctx, patch); err == nil {
		t.Fatal("active source configuration was silently applied")
	}
	if _, _, _, err = control.call(ctx, sourceControlRequest{Operation: "request-keyframe"}); err == nil {
		t.Fatal("unsupported native force-IDR reported success")
	}
	// Execute the checked-in client example against the same real Python source
	// and edge Host, including its SDK discovery, binary stream and DELETE.
	clientContext, clientCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer clientCancel()
	outputFile := filepath.Join(directory, "client-capture.jpg")
	clientCommand := exec.CommandContext(clientContext, "go", "run", filepath.Join("..", "..", "examples", "capture_client"), "--rpc-socket", config.RPCSocket, "--source", "python-camera", "--output", outputFile)
	if output, err := clientCommand.CombinedOutput(); err != nil {
		t.Fatalf("real SDK capture client example: %v: %s", err, output)
	}
	clientMetadata, err := os.ReadFile(outputFile + ".json")
	if err != nil {
		t.Fatal(err)
	}
	var saved snapshotMetadata
	if err = json.Unmarshal(clientMetadata, &saved); err != nil || saved.Sequence != 10 || saved.TimestampNanoseconds != 123456789 || saved.SourceID != "python-camera" {
		t.Fatalf("client example lost same-frame metadata: %s %v", clientMetadata, err)
	}
	if _, retained := edge.Snapshot(saved.SnapshotID); retained {
		t.Fatal("example did not DELETE its retention")
	}
	if control.boundInstance() != first {
		t.Fatal("source instance did not bind discovery")
	}
	// Stop and restart the actual Python source. The old edge lease must fail
	// closed; there is no implicit rediscovery followed by mutation replay.
	if _, err = input.Write([]byte("restart\n")); err != nil {
		t.Fatal(err)
	}
	second := ready()
	restarted = true
	if second == first {
		t.Fatal("source reused incarnation")
	}
	if _, _, _, err = control.call(ctx, sourceControlRequest{Operation: "capture", SnapshotID: "stale"}); err == nil {
		t.Fatal("stale source instance accepted capture")
	}
	// Source restart is an external ownership change. Shutdown must retain
	// uncertainty and return an error until the product owner replaces the edge.
}
