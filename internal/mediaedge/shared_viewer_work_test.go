package mediaedge

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
	mtx "github.com/lxk36/xgc2-media-edge/internal/mediamtx"
)

type measuredSharedMediaControl struct {
	*fakeMediaMTXControl
	pathReads    atomic.Uint64
	sessionReads atomic.Uint64
}

type measuredSourceStatus struct {
	sourceController
	reads atomic.Uint64
}

func (control *measuredSourceStatus) call(ctx context.Context, request sourceControlRequest) (sourceControlResponse, []byte, []byte, error) {
	if request.Operation == "status" {
		control.reads.Add(1)
	}
	return control.sourceController.call(ctx, request)
}

func (control *measuredSharedMediaControl) Paths(ctx context.Context) ([]mtx.PathStatus, error) {
	control.pathReads.Add(1)
	return control.fakeMediaMTXControl.Paths(ctx)
}
func (control *measuredSharedMediaControl) WebRTCSessions(ctx context.Context) ([]mtx.WebRTCSession, error) {
	control.sessionReads.Add(1)
	return control.fakeMediaMTXControl.WebRTCSessions(ctx)
}

type sharedViewerWire struct {
	conn       *net.UDPConn
	packets    atomic.Uint64
	bytes      atomic.Uint64
	mu         sync.Mutex
	timestamps map[uint32]struct{}
	idr        map[uint32]struct{}
}

func newSharedViewerWire(t *testing.T) *sharedViewerWire {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	wire := &sharedViewerWire{conn: conn, timestamps: map[uint32]struct{}{}, idr: map[uint32]struct{}{}}
	go func() {
		packet := make([]byte, 65536)
		for {
			n, _, err := conn.ReadFromUDP(packet)
			if err != nil {
				return
			}
			wire.packets.Add(1)
			wire.bytes.Add(uint64(n))
			if n >= 12 && packet[0]>>6 == 2 {
				stamp := binary.BigEndian.Uint32(packet[4:8])
				wire.mu.Lock()
				wire.timestamps[stamp] = struct{}{}
				if n >= 13 {
					kind := packet[12] & 0x1f
					if kind == 5 || kind == 28 && n >= 14 && packet[13]&0x80 != 0 && packet[13]&0x1f == 5 {
						wire.idr[stamp] = struct{}{}
					}
					if kind == 24 {
						for rest := packet[13:n]; len(rest) >= 3; {
							size := int(binary.BigEndian.Uint16(rest))
							if size == 0 || 2+size > len(rest) {
								break
							}
							if rest[2]&0x1f == 5 {
								wire.idr[stamp] = struct{}{}
							}
							rest = rest[2+size:]
						}
					}
				}
				wire.mu.Unlock()
			}
		}
	}()
	t.Cleanup(func() { _ = conn.Close() })
	return wire
}
func (wire *sharedViewerWire) frames() int {
	wire.mu.Lock()
	defer wire.mu.Unlock()
	return len(wire.timestamps)
}
func (wire *sharedViewerWire) idrFrames() int {
	wire.mu.Lock()
	defer wire.mu.Unlock()
	return len(wire.idr)
}

func processWrittenBytes(t *testing.T, pid int) uint64 {
	t.Helper()
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/io", pid))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "wchar:") {
			value, err := strconv.ParseUint(strings.TrimSpace(strings.TrimPrefix(line, "wchar:")), 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			return value
		}
	}
	t.Fatalf("no wchar for producer %d", pid)
	return 0
}

func waitSharedViewer(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("shared viewer work did not converge")
}

// Real raw-frame producer + real GStreamer H264 encoder + actual Edge HTTP and
// UDP. MediaMTX WHEP control is the existing deterministic fake: this does not
// claim actual ICE/SRTP or a physical camera.
func TestSharedViewersUseOneRealCaptureAndEncoderUntilLastRelease(t *testing.T) {
	requireGStreamer(t, "fdsrc", "rawvideoparse", "videoconvert", "openh264enc", "h264parse", "rtph264pay", "udpsink")
	wire := newSharedViewerWire(t)
	templates, runtime := t.TempDir(), t.TempDir()
	producerPID := filepath.Join(runtime, "capture.pid")
	template := map[string]any{
		"name": "actual-h264", "description": "real encoder test", "input": "stdin", "snapshot": false, "parameters": map[string]any{},
		"command": []string{"gst-launch-1.0", "-q", "fdsrc", "fd=0", "!", "rawvideoparse", "format=bgr", "width=${width}", "height=${height}", "framerate=${fps}/1", "!", "videoconvert", "!", "video/x-raw,format=I420", "!", "openh264enc", "gop-size=${fps}", "bitrate=800000", "!", "h264parse", "config-interval=1", "!", "rtph264pay", "pt=${payloadType}", "config-interval=1", "mtu=1200", "!", "udpsink", "host=${rtpHost}", "port=${rtpPort}", "sync=false", "async=false"},
	}
	encoded, err := json.Marshal(template)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(templates, "actual-h264.json"), encoded, 0600); err != nil {
		t.Fatal(err)
	}
	producer := []string{testBinary(t), "fake-video", "--width=160", "--height=120", "--fps=10", "--pid-file=" + producerPID}
	source := pipelineTestSource("cam", wire.conn.LocalAddr().String(), "actual-h264", nil, producer)
	source.Width, source.Height = 160, 120
	parked := pipelineTestSource("parked", availableLoopbackRTPAddress(t), "actual-h264", nil, producer)
	parked.Width, parked.Height = 160, 120
	config, err := (Config{ControlAddress: "127.0.0.1:0", PipelineTemplatesDir: templates, PipelineRuntimeDir: filepath.Join(runtime, "pipelines"), Sources: []SourceConfig{source, parked}}).normalized()
	if err != nil {
		t.Fatal(err)
	}
	logs := &lockedBuffer{}
	native := &measuredSharedMediaControl{fakeMediaMTXControl: newFakeMediaMTXControl("cam", "parked")}
	server := newMediaMTXServer(config, MediaMTXSettings{Stderr: logs}, native, newFakeMediaMTXProcess())
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	control := server.sources["cam"].controller().(*pipelineControl)
	activeStatus := &measuredSourceStatus{sourceController: control}
	parkedStatus := &measuredSourceStatus{sourceController: server.sources["parked"].controller()}
	server.sources["cam"].controlRPC = activeStatus
	server.sources["parked"].controlRPC = parkedStatus
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	open := func() SessionAnswer {
		t.Helper()
		response, err := http.Post("http://"+server.ControlAddress()+"/api/v1/sources/cam/sessions", "application/json", bytes.NewBufferString(`{"sdp":"v=0\r\n"}`))
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusCreated {
			t.Fatalf("open: %s logs=%s", response.Status, logs.String())
		}
		var answer SessionAnswer
		if err := json.NewDecoder(response.Body).Decode(&answer); err != nil {
			t.Fatal(err)
		}
		return answer
	}
	closeView := func(answer SessionAnswer) {
		t.Helper()
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodDelete, "http://"+server.ControlAddress()+"/api/v1/sessions/"+answer.SessionID, nil)
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusNoContent {
			t.Fatalf("close: %s", response.Status)
		}
	}
	if _, err := os.Stat(producerPID); !os.IsNotExist(err) || wire.packets.Load() != 0 {
		t.Fatal("source worked before demand")
	}
	time.Sleep(350 * time.Millisecond)
	if wire.packets.Load() != 0 || native.pathReads.Load() != 0 || native.sessionReads.Load() != 0 {
		t.Fatal("idle source performed optional work before demand")
	}
	t.Log("before demand: producer=0 encoder=0 RTP packets=0 inventory reads=0")
	first := open()
	producer1 := readPID(t, producerPID)
	_, port, _ := net.SplitHostPort(wire.conn.LocalAddr().String())
	encoderIDs := processesWithArgument("port=" + port)
	if len(encoderIDs) != 1 {
		t.Fatalf("first viewer encoders=%v logs=%s", encoderIDs, logs.String())
	}
	encoder1 := encoderIDs[0]
	waitSharedViewer(t, 3*time.Second, func() bool { return wire.frames() >= 5 })
	control.mu.Lock()
	firstStarted := control.startedAt
	control.mu.Unlock()
	for time.Since(firstStarted) < pipelineKeyframeRestartAge+100*time.Millisecond {
		time.Sleep(25 * time.Millisecond)
	}
	beforeBytes, beforePackets, beforeFrames, beforeIDR := wire.bytes.Load(), wire.packets.Load(), wire.frames(), wire.idrFrames()
	beforeInput := processWrittenBytes(t, producer1)
	second := open()
	time.Sleep(1300 * time.Millisecond)
	control.mu.Lock()
	generation := control.generation
	control.mu.Unlock()
	producers := readPID(t, producerPID)
	encoders := processesWithArgument("port=" + port)
	inputAfter := processWrittenBytes(t, producers)
	t.Logf("while one source is visible: live status reads=%d unrelated parked status reads=%d", activeStatus.reads.Load(), parkedStatus.reads.Load())
	if parkedStatus.reads.Load() != 0 {
		t.Error("visible source caused unrelated parked source status reads")
	}
	t.Logf("two views: generation=%d producer=%d->%d encoder=%d->%v producer_wchar=%d->%d RTP_packets=%d RTP_bytes=%d encoded_frames=%d IDR_frames=%d", generation, producer1, producers, encoder1, encoders, beforeInput, inputAfter, wire.packets.Load()-beforePackets, wire.bytes.Load()-beforeBytes, wire.frames()-beforeFrames, wire.idrFrames()-beforeIDR)
	if wire.idrFrames() == beforeIDR {
		t.Error("periodic H264 keyframes did not continue for the added viewer")
	}
	if generation != 1 || producers != producer1 || len(encoders) != 1 || encoders[0] != encoder1 {
		t.Error("adding a viewer restarted the shared capture/encoder")
		if len(encoders) != 1 {
			t.Fatalf("unexpected encoder processes: %v", encoders)
		}
		producer1, encoder1 = producers, encoders[0]
	}
	closeView(first)
	time.Sleep(250 * time.Millisecond)
	if processGone(producer1) || processGone(encoder1) || readPID(t, producerPID) != producer1 {
		t.Fatal("one hidden viewer stopped the other viewer's source")
	}
	closeView(second)
	lastPackets, lastBytes := wire.packets.Load(), wire.bytes.Load()
	lastPaths, lastSessions := native.pathReads.Load(), native.sessionReads.Load()
	time.Sleep(400 * time.Millisecond)
	t.Logf("400ms after last close: producer_alive=%t encoder_alive=%t RTP_packets=%d RTP_bytes=%d native_path_reads=%d native_session_reads=%d", !processGone(producer1), !processGone(encoder1), wire.packets.Load()-lastPackets, wire.bytes.Load()-lastBytes, native.pathReads.Load()-lastPaths, native.sessionReads.Load()-lastSessions)
	waitSharedViewer(t, 3*time.Second, func() bool { return processGone(producer1) && processGone(encoder1) })
	status := server.SourceStatuses()[0]
	if status.NativeState != "idle" || status.NativeAppliedActive || status.Active || status.Available || status.Online {
		t.Fatalf("idle source status stale: %+v", status)
	}
	// A MediaMTX read initiated before the stop may complete afterwards.
	// Inject that late path fact and verify it cannot resurrect the lease.
	server.sources["cam"].observePath(time.Now(), mtx.PathStatus{Name: "cam", Available: true, Online: true, InboundBytes: status.BytesReceived})
	if afterLatePath := server.SourceStatuses()[0]; afterLatePath.Available || afterLatePath.Online {
		t.Fatalf("late path observation resurrected stopped source: %+v", afterLatePath)
	}
	time.Sleep(100 * time.Millisecond)
	parkedPackets, parkedBytes := wire.packets.Load(), wire.bytes.Load()
	parkedPaths, parkedSessions := native.pathReads.Load(), native.sessionReads.Load()
	parkedActiveStatus := activeStatus.reads.Load()
	time.Sleep(300 * time.Millisecond)
	if wire.packets.Load() != parkedPackets || wire.bytes.Load() != parkedBytes {
		t.Fatal("hidden source sent RTP after last owner joined")
	}
	if native.pathReads.Load() != parkedPaths || native.sessionReads.Load() != parkedSessions || activeStatus.reads.Load() != parkedActiveStatus {
		t.Fatal("idle edge continued optional MediaMTX inventory reads")
	}
	t.Logf("last release: producer=0 encoder=0 after-join RTP packets=%d bytes=%d local status reads=%d", wire.packets.Load()-parkedPackets, wire.bytes.Load()-parkedBytes, activeStatus.reads.Load()-parkedActiveStatus)
	third := open()
	producer2 := readPID(t, producerPID)
	if producer2 == producer1 {
		t.Fatal("reveal did not reattach a current producer")
	}
	waitSharedViewer(t, 3*time.Second, func() bool { return wire.packets.Load() > parkedPackets })
	closeView(third)
	waitSharedViewer(t, 3*time.Second, func() bool { return processGone(producer2) })
	native.mu.Lock()
	opened, active := native.next, len(native.sessions)
	native.mu.Unlock()
	if opened != 3 || active != 0 {
		t.Fatalf("WHEP opened=%d active=%d", opened, active)
	}
	t.Logf("reveal/join: offers=%d active peers=%d producer=0", opened, active)
}

// The camera peer speaks its actual XRPC/HTTP protocol over a Unix socket;
// only the camera implementation and MediaMTX are fixtures. Byte counts are
// request/response bodies at that boundary, not AgentLink network packets.
func TestSharedCameraViewersUseOneNativeSourceLeaseAndIdleWithoutIO(t *testing.T) {
	directory, err := os.MkdirTemp("", "camera-shared-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	socket := filepath.Join(directory, "camera.sock")
	address := availableLoopbackRTPAddress(t)
	_, portText, _ := net.SplitHostPort(address)
	description := defaultCaptureDescription()
	description.RTPPort, _ = strconv.Atoi(portText)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	counts := map[string]int{}
	requestBytes, responseBytes := 0, 0
	active := false
	camera := &http.Server{Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Error(err)
			return
		}
		operation := pathBase(request.URL.Path)
		writer.Header().Set("X-Xrpc-Instance-ID", "shared-instance")
		writer.Header().Set("Content-Type", "application/json")
		var result any
		mu.Lock()
		counts[operation]++
		requestBytes += len(body)
		if request.URL.Path == "/v1/describe" {
			result = cameraDiscovery(xrpc.ServiceRef{TargetID: "peer-test", Service: "camera-source", APIVersion: "v1", InstanceID: "shared-instance", Profile: xrpc.HTTP, Endpoint: xrpc.Endpoint{Kind: "unix", Address: socket}})
		} else if operation == "describe" {
			result = description
		} else {
			if operation == "start" {
				active = true
			}
			if operation == "stop" {
				active = false
			}
			state := "idle"
			if active {
				state = "active"
			}
			result = sourceControlResponse{OK: true, ManagedSourceID: "camera", Active: active, AppliedActive: active, State: state, Completion: "applied", ConfigurationRevision: 1}
		}
		encoded, _ := json.Marshal(result)
		responseBytes += len(encoded)
		mu.Unlock()
		_, _ = writer.Write(encoded)
	})}
	go camera.Serve(listener)
	t.Cleanup(func() { _ = camera.Close() })
	config, err := (Config{TargetID: "peer-test", ControlAddress: "127.0.0.1:0", RPCSocket: filepath.Join(directory, "edge.sock"), Sources: []SourceConfig{{ID: "camera", ControlSocket: socket, RTPListenAddress: address}}}).normalized()
	if err != nil {
		t.Fatal(err)
	}
	media := &measuredSharedMediaControl{fakeMediaMTXControl: newFakeMediaMTXControl("camera")}
	edge := newMediaMTXServer(config, MediaMTXSettings{Stderr: &lockedBuffer{}}, media, newFakeMediaMTXProcess())
	if err := edge.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := edge.Close(); err != nil {
			t.Error(err)
		}
	})
	snapshot := func() (map[string]int, int, int) {
		mu.Lock()
		defer mu.Unlock()
		copyCounts := map[string]int{}
		for key, value := range counts {
			copyCounts[key] = value
		}
		return copyCounts, requestBytes, responseBytes
	}
	first, err := edge.OpenSession(context.Background(), "camera", SessionOffer{SDP: "v=0\r\n"})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(350 * time.Millisecond)
	one, _, _ := snapshot()
	second, err := edge.OpenSession(context.Background(), "camera", SessionOffer{SDP: "v=0\r\n"})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(350 * time.Millisecond)
	two, req, resp := snapshot()
	if two["start"] != 1 || two["capture"] != 0 || two["describe"] != one["describe"] || two["request-keyframe"] != 2 {
		t.Fatalf("second viewer created source work: first=%v second=%v", one, two)
	}
	t.Logf("two peers, one source: XRPC ops=%v request_body_bytes=%d response_body_bytes=%d", two, req, resp)
	if found, err := edge.CloseSession(first.SessionID); !found || err != nil {
		t.Fatalf("close first %v %v", found, err)
	}
	time.Sleep(300 * time.Millisecond)
	oneLeft, _, _ := snapshot()
	if oneLeft["stop"] != 0 {
		t.Fatalf("first close stopped active second viewer: %v", oneLeft)
	}
	if found, err := edge.CloseSession(second.SessionID); !found || err != nil {
		t.Fatalf("close second %v %v", found, err)
	}
	waitSharedViewer(t, 3*time.Second, func() bool { state, _, _ := snapshot(); return state["stop"] == 1 })
	time.Sleep(100 * time.Millisecond)
	parked, parkedReq, parkedResp := snapshot()
	parkedPaths, parkedSessions := media.pathReads.Load(), media.sessionReads.Load()
	time.Sleep(400 * time.Millisecond)
	after, afterReq, afterResp := snapshot()
	if !reflect.DeepEqual(parked, after) || afterReq != parkedReq || afterResp != parkedResp || media.pathReads.Load() != parkedPaths || media.sessionReads.Load() != parkedSessions {
		t.Fatalf("hidden source generated more control work: before=%v/%d/%d after=%v/%d/%d", parked, parkedReq, parkedResp, after, afterReq, afterResp)
	}
	t.Logf("last release: XRPC body bytes delta=%d/%d, native inventory reads delta=%d/%d", afterReq-parkedReq, afterResp-parkedResp, media.pathReads.Load()-parkedPaths, media.sessionReads.Load()-parkedSessions)
	third, err := edge.OpenSession(context.Background(), "camera", SessionOffer{SDP: "v=0\r\n"})
	if err != nil {
		t.Fatal(err)
	}
	if found, err := edge.CloseSession(third.SessionID); !found || err != nil {
		t.Fatalf("close third %v %v", found, err)
	}
	waitSharedViewer(t, 3*time.Second, func() bool { state, _, _ := snapshot(); return state["stop"] == 2 })
	final, _, _ := snapshot()
	if final["start"] != 2 || final["stop"] != 2 {
		t.Fatalf("reveal/join leases=%v", final)
	}
	t.Logf("reveal/join: starts=%d stops=%d", final["start"], final["stop"])
}

func TestCameraPeerIdleSocketReplacementDoesNotReconnectWithoutDemand(t *testing.T) {
	edge, socket, address, _ := cameraPeerServer(t)
	camera := startRestartingCamera(t, socket, address, "idle-first")
	answer, err := edge.OpenSession(context.Background(), "camera", SessionOffer{SDP: "offer"})
	if err != nil {
		t.Fatal(err)
	}
	if found, err := edge.CloseSession(answer.SessionID); !found || err != nil {
		t.Fatalf("close %v %v", found, err)
	}
	waitSharedViewer(t, 3*time.Second, func() bool { return camera.stops.Load() == 1 })
	peer := edge.sources["camera"].controller().(*cameraPeer)
	peer.mu.Lock()
	before := peer.attempts
	peer.mu.Unlock()
	camera.close()
	time.Sleep(800 * time.Millisecond)
	peer.mu.Lock()
	after := peer.attempts
	peer.mu.Unlock()
	t.Logf("parked source loss: discovery attempts before=%d after=%d", before, after)
	if after != before {
		t.Fatal("idle source reconnected without demand")
	}
	replacement := startRestartingCamera(t, socket, address, "idle-replacement")
	// A genuinely new incarnation must receive the previously accepted stop
	// once, so a replacement cannot resurrect the retired source lease.
	waitSharedViewer(t, 3*time.Second, func() bool { return replacement.stops.Load() == 1 })
	peer.mu.Lock()
	replacementAttempts := peer.attempts
	peer.mu.Unlock()
	if replacementAttempts != before+1 || replacement.starts.Load() != 0 {
		t.Fatalf("replacement lease reconciliation: attempts=%d starts=%d stops=%d", replacementAttempts-before, replacement.starts.Load(), replacement.stops.Load())
	}
	time.Sleep(400 * time.Millisecond)
	peer.mu.Lock()
	stableAttempts := peer.attempts
	peer.mu.Unlock()
	if stableAttempts != replacementAttempts || replacement.stops.Load() != 1 {
		t.Fatal("idle replacement continued work after the stop was applied")
	}
	t.Logf("replacement incarnation: discovery=1 stop=1, stable idle additional discovery=0 stop=0")
	revealed, err := edge.OpenSession(context.Background(), "camera", SessionOffer{SDP: "offer"})
	if err != nil {
		t.Fatalf("ready source refused revealed viewer: %v", err)
	}
	if found, err := edge.CloseSession(revealed.SessionID); !found || err != nil {
		t.Fatalf("close revealed %v %v", found, err)
	}
	waitSharedViewer(t, 3*time.Second, func() bool { return replacement.stops.Load() == 2 })
}
