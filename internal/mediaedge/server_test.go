package mediaedge

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

func TestNewRejectsExternallyReachableControlAndIngress(t *testing.T) {
	_, err := New(Config{
		ControlAddress: "0.0.0.0:18083",
		Sources: []SourceConfig{{
			ID: "camera", RTPListenAddress: "127.0.0.1:5004", ControlSocket: "/tmp/camera.sock",
			Width: 1280, Height: 720, FPS: 20,
		}},
	})
	if err == nil {
		t.Fatal("external media control address was accepted")
	}
	_, err = New(Config{
		ControlAddress: "127.0.0.1:18083",
		Sources: []SourceConfig{{
			ID: "camera", RTPListenAddress: "0.0.0.0:5004", ControlSocket: "/tmp/camera.sock",
			Width: 1280, Height: 720, FPS: 20,
		}},
	})
	if err == nil {
		t.Fatal("external media RTP ingress was accepted")
	}
}

func TestH264CapabilityRequiresLevel51AndKeyframeFeedback(t *testing.T) {
	capability := h264CodecCapability()
	if capability.SDPFmtpLine != h264FMTP ||
		!strings.Contains(capability.SDPFmtpLine, "profile-level-id=42e033") {
		t.Fatalf("unexpected H264 capability: %+v", capability)
	}
	required := map[webrtc.RTCPFeedback]bool{
		{Type: webrtc.TypeRTCPFBCCM, Parameter: "fir"}:  false,
		{Type: webrtc.TypeRTCPFBNACK}:                   false,
		{Type: webrtc.TypeRTCPFBNACK, Parameter: "pli"}: false,
	}
	for _, feedback := range capability.RTCPFeedback {
		if _, found := required[feedback]; found {
			required[feedback] = true
		}
	}
	for feedback, found := range required {
		if !found {
			t.Fatalf("H264 capability does not advertise %+v", feedback)
		}
	}
}

func TestSourceLifecycleSnapshotAndRTPIngress(t *testing.T) {
	capture := newCaptureControl(t)
	defer capture.close()
	server := newTestServer(t, capture.socket)
	defer server.Close()

	snapshot, err := server.CaptureSnapshot(context.Background(), "camera")
	if err != nil {
		t.Fatalf("capture snapshot: %v", err)
	}
	if snapshot.ID == "" || string(snapshot.JPEG) != "\xff\xd8xgc\xff\xd9" || len(snapshot.RGB) != 16*16*3 {
		t.Fatalf("unexpected snapshot: %+v", snapshot)
	}
	if found, ok := server.Snapshot(snapshot.ID); !ok || found.SourceID != "camera" {
		t.Fatalf("snapshot was not retained: %+v, %v", found, ok)
	}
	if !server.DeleteSnapshot(snapshot.ID) {
		t.Fatal("snapshot was not deleted")
	}
	if _, ok := server.Snapshot(snapshot.ID); ok {
		t.Fatal("deleted snapshot remained readable")
	}
	capture.waitFor(t, 1, func(request sourceControlRequest) bool {
		return request.Operation == "set-active" && request.Active != nil && *request.Active
	})
	capture.waitFor(t, 1, func(request sourceControlRequest) bool { return request.Operation == "snapshot" })
	capture.waitFor(t, 1, func(request sourceControlRequest) bool {
		return request.Operation == "set-active" && request.Active != nil && !*request.Active
	})

	connection, err := net.Dial("udp", server.RTPAddress("camera"))
	if err != nil {
		t.Fatalf("dial RTP ingress: %v", err)
	}
	defer connection.Close()
	packet, err := (&rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 96, SequenceNumber: 7, Timestamp: 90_000, SSRC: 42}, Payload: []byte{0x65, 0x01}}).Marshal()
	if err != nil {
		t.Fatalf("marshal RTP: %v", err)
	}
	if _, err := connection.Write(packet); err != nil {
		t.Fatalf("write RTP: %v", err)
	}
	eventually(t, time.Second, func() bool { return server.SourceStatuses()[0].PacketsReceived == 1 })
}

func TestOpenAndCloseWebRTCSessionReferenceCountsCapture(t *testing.T) {
	capture := newCaptureControl(t)
	defer capture.close()
	server := newTestServer(t, capture.socket)
	defer server.Close()

	browser, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create browser peer: %v", err)
	}
	defer browser.Close()
	if _, err := browser.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionRecvonly,
	}); err != nil {
		t.Fatalf("add browser video transceiver: %v", err)
	}
	offer, err := browser.CreateOffer(nil)
	if err != nil {
		t.Fatalf("create browser offer: %v", err)
	}
	gather := webrtc.GatheringCompletePromise(browser)
	if err := browser.SetLocalDescription(offer); err != nil {
		t.Fatalf("set browser offer: %v", err)
	}
	select {
	case <-gather:
	case <-time.After(5 * time.Second):
		t.Fatal("browser ICE gathering timed out")
	}
	answer, err := server.OpenSession(context.Background(), "camera", SessionOffer{SDP: browser.LocalDescription().SDP})
	if err != nil {
		t.Fatalf("open media session: %v", err)
	}
	if answer.SessionID == "" || answer.DataChannelLabel != ControlDataChannelLabel || answer.Source.Codec != "H264" {
		t.Fatalf("unexpected media session answer: %+v", answer)
	}
	if !strings.Contains(answer.SDP, " nack pli") || !strings.Contains(answer.SDP, " ccm fir") {
		t.Fatalf("media answer does not negotiate PLI/FIR feedback:\n%s", answer.SDP)
	}
	capture.waitFor(t, 1, func(request sourceControlRequest) bool {
		return request.Operation == "set-active" && request.Active != nil && *request.Active
	})
	capture.expectNoMatch(t, 75*time.Millisecond, func(request sourceControlRequest) bool {
		return request.Operation == "request-keyframe"
	})
	if err := browser.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answer.SDP}); err != nil {
		t.Fatalf("set browser answer: %v", err)
	}
	eventually(t, time.Second, func() bool { return server.SourceStatuses()[0].Consumers == 1 })
	capture.waitFor(t, 1, func(request sourceControlRequest) bool {
		return request.Operation == "request-keyframe"
	})
	if !server.CloseSession(answer.SessionID) {
		t.Fatal("close media session reported an unknown session")
	}
	eventually(t, time.Second, func() bool { return server.SourceStatuses()[0].Consumers == 0 })
	capture.waitFor(t, 1, func(request sourceControlRequest) bool {
		return request.Operation == "set-active" && request.Active != nil && !*request.Active
	})
}

func TestRTCPKeyframeFeedbackIsRateLimited(t *testing.T) {
	capture := newCaptureControl(t)
	defer capture.close()
	server := newTestServer(t, capture.socket)
	defer server.Close()

	browser, answer := openConnectedBrowserSession(t, server)
	defer server.CloseSession(answer.SessionID)
	capture.waitFor(t, 1, func(request sourceControlRequest) bool {
		return request.Operation == "set-active" && request.Active != nil && *request.Active
	})
	// PeerConnectionStateConnected forces one IDR after the SRTP path is ready.
	capture.waitFor(t, 1, func(request sourceControlRequest) bool {
		return request.Operation == "request-keyframe"
	})
	mediaSSRC := sessionMediaSSRC(t, server, answer.SessionID)

	time.Sleep(keyframeRequestMinimumInterval + 25*time.Millisecond)
	if err := browser.WriteRTCP([]rtcp.Packet{
		&rtcp.PictureLossIndication{SenderSSRC: 1, MediaSSRC: mediaSSRC},
	}); err != nil {
		t.Fatalf("write PLI: %v", err)
	}
	capture.waitFor(t, 1, func(request sourceControlRequest) bool {
		return request.Operation == "request-keyframe"
	})

	// A compound/burst feedback packet must not amplify into source-control
	// traffic while the source-wide minimum interval is active.
	if err := browser.WriteRTCP([]rtcp.Packet{
		&rtcp.PictureLossIndication{SenderSSRC: 1, MediaSSRC: mediaSSRC},
		&rtcp.FullIntraRequest{
			SenderSSRC: 1, MediaSSRC: mediaSSRC,
			FIR: []rtcp.FIREntry{{SSRC: mediaSSRC, SequenceNumber: 1}},
		},
	}); err != nil {
		t.Fatalf("write feedback burst: %v", err)
	}
	capture.expectNoMatch(t, 100*time.Millisecond, func(request sourceControlRequest) bool {
		return request.Operation == "request-keyframe"
	})

	time.Sleep(keyframeRequestMinimumInterval)
	if err := browser.WriteRTCP([]rtcp.Packet{
		&rtcp.FullIntraRequest{
			SenderSSRC: 1, MediaSSRC: mediaSSRC,
			FIR: []rtcp.FIREntry{{SSRC: mediaSSRC, SequenceNumber: 2}},
		},
	}); err != nil {
		t.Fatalf("write FIR: %v", err)
	}
	capture.waitFor(t, 1, func(request sourceControlRequest) bool {
		return request.Operation == "request-keyframe"
	})
}

func TestActiveSourceRecoversAfterCaptureRestart(t *testing.T) {
	capture := newCaptureControl(t)
	server := newTestServer(t, capture.socket)
	defer server.Close()

	browser, answer := openConnectedBrowserSession(t, server)
	defer browser.Close()
	defer server.CloseSession(answer.SessionID)
	capture.waitFor(t, 1, func(request sourceControlRequest) bool {
		return request.Operation == "set-active" && request.Active != nil && *request.Active
	})
	capture.waitFor(t, 1, func(request sourceControlRequest) bool {
		return request.Operation == "request-keyframe"
	})

	socket := capture.socket
	capture.close()
	item := server.source("camera")
	item.mu.Lock()
	item.stallTimeout = 50 * time.Millisecond
	item.recoveryMinimumInterval = 50 * time.Millisecond
	item.activeSince = time.Now().Add(-time.Second)
	item.mu.Unlock()

	// Let at least one recovery attempt observe the missing control socket.
	time.Sleep(sourceWatchInterval + 50*time.Millisecond)
	restarted := newCaptureControlAt(t, socket)
	defer restarted.close()
	restarted.waitFor(t, 1, func(request sourceControlRequest) bool {
		return request.Operation == "set-active" && request.Active != nil && *request.Active
	})
	restarted.waitFor(t, 1, func(request sourceControlRequest) bool {
		return request.Operation == "request-keyframe"
	})
}

func TestPendingConsumerPreventsSourceDeactivation(t *testing.T) {
	capture := newCaptureControl(t)
	defer capture.close()
	server := newTestServer(t, capture.socket)
	defer server.Close()
	item := server.source("camera")

	if err := item.acquire(context.Background()); err != nil {
		t.Fatalf("reserve pending source consumer: %v", err)
	}
	capture.waitFor(t, 1, func(request sourceControlRequest) bool {
		return request.Operation == "set-active" && request.Active != nil && *request.Active
	})
	// A concurrent failed offer may ask for deactivation while this consumer is
	// still gathering. The pending reservation must keep that timer disarmed.
	item.scheduleDeactivate()
	capture.expectNoMatch(t, 25*time.Millisecond, func(request sourceControlRequest) bool {
		return request.Operation == "set-active" && request.Active != nil && !*request.Active
	})

	item.releasePending("pending-session")
	capture.expectNoMatch(t, 25*time.Millisecond, func(request sourceControlRequest) bool {
		return request.Operation == "set-active" && request.Active != nil && !*request.Active
	})

	item.removeSession("pending-session")
	capture.waitFor(t, 1, func(request sourceControlRequest) bool {
		return request.Operation == "set-active" && request.Active != nil && !*request.Active
	})
}

func TestDeactivateSerializesWithNewSourceConsumer(t *testing.T) {
	releaseDeactivate := make(chan struct{})
	var releaseDeactivateOnce sync.Once
	releaseDeactivateResponse := func() {
		releaseDeactivateOnce.Do(func() { close(releaseDeactivate) })
	}
	capture := newCaptureControlWithHook(t, func(request sourceControlRequest) {
		if request.Operation == "set-active" && request.Active != nil && !*request.Active {
			<-releaseDeactivate
		}
	})
	defer capture.close()
	defer releaseDeactivateResponse()
	server := newTestServer(t, capture.socket)
	defer server.Close()
	item := server.source("camera")

	if err := item.acquire(context.Background()); err != nil {
		t.Fatalf("activate source: %v", err)
	}
	capture.waitFor(t, 1, func(request sourceControlRequest) bool {
		return request.Operation == "set-active" && request.Active != nil && *request.Active
	})
	item.releasePending("")
	capture.waitFor(t, 1, func(request sourceControlRequest) bool {
		return request.Operation == "set-active" && request.Active != nil && !*request.Active
	})

	acquired := make(chan error, 1)
	go func() {
		acquired <- item.acquire(context.Background())
	}()
	select {
	case err := <-acquired:
		t.Fatalf("new consumer bypassed in-flight deactivation: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	releaseDeactivateResponse()
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatalf("reactivate source after deactivation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("new source consumer remained blocked after deactivation completed")
	}
	capture.waitFor(t, 1, func(request sourceControlRequest) bool {
		return request.Operation == "set-active" && request.Active != nil && *request.Active
	})
	item.releasePending("")
}

func TestCloseCancelsAndWaitsForPendingSessionActivation(t *testing.T) {
	releaseActivation := make(chan struct{})
	capture := newCaptureControlWithHook(t, func(request sourceControlRequest) {
		if request.Operation == "set-active" && request.Active != nil && *request.Active {
			<-releaseActivation
		}
	})
	defer capture.close()
	defer close(releaseActivation)
	server := newTestServer(t, capture.socket)

	openResult := make(chan error, 1)
	go func() {
		_, err := server.OpenSession(context.Background(), "camera", SessionOffer{SDP: "v=0\r\n"})
		openResult <- err
	}()
	capture.waitFor(t, 1, func(request sourceControlRequest) bool {
		return request.Operation == "set-active" && request.Active != nil && *request.Active
	})

	closeResult := make(chan error, 1)
	go func() {
		closeResult <- server.Close()
	}()
	select {
	case err := <-openResult:
		if err == nil {
			t.Fatal("pending OpenSession succeeded while the server was closing")
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel the pending source activation")
	}
	select {
	case err := <-closeResult:
		if err != nil {
			t.Fatalf("close media edge: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not wait for and drain the pending OpenSession")
	}

	server.mu.RLock()
	retainedSessions := len(server.sessions)
	server.mu.RUnlock()
	if retainedSessions != 0 {
		t.Fatalf("sessions retained after Close: %d", retainedSessions)
	}
	if _, err := server.OpenSession(context.Background(), "camera", SessionOffer{SDP: "v=0\r\n"}); err == nil ||
		!strings.Contains(err.Error(), "closing") {
		t.Fatalf("OpenSession after Close returned %v, want closing error", err)
	}
}

func TestSourceControlAppliesHardTimeoutWithoutCallerDeadline(t *testing.T) {
	releaseResponse := make(chan struct{})
	capture := newCaptureControlWithHook(t, func(request sourceControlRequest) {
		if request.Operation == "set-active" {
			<-releaseResponse
		}
	})
	defer capture.close()
	defer close(releaseResponse)

	active := true
	started := time.Now()
	_, _, _, err := callSourceControl(context.Background(), capture.socket, sourceControlRequest{
		Operation: "set-active", Active: &active,
	})
	elapsed := time.Since(started)
	if err == nil {
		t.Fatal("source control without a caller deadline did not time out")
	}
	if elapsed < sourceControlRequestTimeout/2 || elapsed > sourceControlRequestTimeout+time.Second {
		t.Fatalf("source control timeout took %s, want approximately %s", elapsed, sourceControlRequestTimeout)
	}
}

func openConnectedBrowserSession(t *testing.T, server *Server) (*webrtc.PeerConnection, SessionAnswer) {
	t.Helper()
	browser, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create browser peer: %v", err)
	}
	t.Cleanup(func() { _ = browser.Close() })
	connected := make(chan struct{})
	var connectedOnce sync.Once
	browser.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateConnected {
			connectedOnce.Do(func() { close(connected) })
		}
	})
	if _, err := browser.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionRecvonly,
	}); err != nil {
		t.Fatalf("add browser video transceiver: %v", err)
	}
	offer, err := browser.CreateOffer(nil)
	if err != nil {
		t.Fatalf("create browser offer: %v", err)
	}
	gather := webrtc.GatheringCompletePromise(browser)
	if err := browser.SetLocalDescription(offer); err != nil {
		t.Fatalf("set browser offer: %v", err)
	}
	select {
	case <-gather:
	case <-time.After(5 * time.Second):
		t.Fatal("browser ICE gathering timed out")
	}
	answer, err := server.OpenSession(context.Background(), "camera", SessionOffer{
		SDP: browser.LocalDescription().SDP,
	})
	if err != nil {
		t.Fatalf("open media session: %v", err)
	}
	if err := browser.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer, SDP: answer.SDP,
	}); err != nil {
		t.Fatalf("set browser answer: %v", err)
	}
	select {
	case <-connected:
	case <-time.After(10 * time.Second):
		t.Fatal("browser WebRTC connection timed out")
	}
	return browser, answer
}

func sessionMediaSSRC(t *testing.T, server *Server, sessionID string) uint32 {
	t.Helper()
	server.mu.RLock()
	item := server.sessions[sessionID]
	server.mu.RUnlock()
	if item == nil {
		t.Fatal("media session was not retained")
	}
	senders := item.peer.GetSenders()
	if len(senders) != 1 {
		t.Fatalf("expected one RTP sender, got %d", len(senders))
	}
	parameters := senders[0].GetParameters()
	if len(parameters.Encodings) != 1 || parameters.Encodings[0].SSRC == 0 {
		t.Fatalf("media sender does not have one SSRC: %+v", parameters.Encodings)
	}
	return uint32(parameters.Encodings[0].SSRC)
}

func newTestServer(t *testing.T, socket string) *Server {
	t.Helper()
	server, err := New(Config{
		ControlAddress:       "127.0.0.1:0",
		SessionGracePeriod:   5 * time.Millisecond,
		SessionGatherTimeout: 3 * time.Second,
		Sources: []SourceConfig{{
			ID: "camera", RTPListenAddress: "127.0.0.1:0", ControlSocket: socket,
			Width: 16, Height: 16, FPS: 20, FrameID: "camera_optical",
		}},
	})
	if err != nil {
		t.Fatalf("create media edge: %v", err)
	}
	if err := server.Start(); err != nil {
		t.Fatalf("start media edge: %v", err)
	}
	// Keep unrelated WebRTC timing tests independent from the production stall
	// watchdog. The restart test below explicitly installs a short timeout.
	source := server.source("camera")
	source.mu.Lock()
	source.stallTimeout = time.Hour
	source.mu.Unlock()
	return server
}

type captureControl struct {
	socket      string
	listener    net.Listener
	requests    chan sourceControlRequest
	closed      chan struct{}
	beforeReply func(sourceControlRequest)
	once        sync.Once
}

func newCaptureControl(t *testing.T) *captureControl {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "camera.sock")
	return newCaptureControlAtWithHook(t, socket, nil)
}

func newCaptureControlWithHook(t *testing.T, beforeReply func(sourceControlRequest)) *captureControl {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "camera.sock")
	return newCaptureControlAtWithHook(t, socket, beforeReply)
}

func newCaptureControlAt(t *testing.T, socket string) *captureControl {
	t.Helper()
	return newCaptureControlAtWithHook(t, socket, nil)
}

func newCaptureControlAtWithHook(
	t *testing.T,
	socket string,
	beforeReply func(sourceControlRequest),
) *captureControl {
	t.Helper()
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("listen capture control: %v", err)
	}
	control := &captureControl{
		socket: socket, listener: listener, requests: make(chan sourceControlRequest, 16),
		closed: make(chan struct{}), beforeReply: beforeReply,
	}
	go control.accept()
	return control
}

func (control *captureControl) accept() {
	for {
		connection, err := control.listener.Accept()
		if err != nil {
			return
		}
		go control.handle(connection)
	}
}

func (control *captureControl) handle(connection net.Conn) {
	defer connection.Close()
	line, err := bufio.NewReader(connection).ReadString('\n')
	if err != nil {
		return
	}
	var request sourceControlRequest
	if json.Unmarshal([]byte(line), &request) != nil {
		return
	}
	select {
	case control.requests <- request:
	case <-control.closed:
		return
	}
	if control.beforeReply != nil {
		control.beforeReply(request)
	}
	if request.Operation != "snapshot" {
		_, _ = connection.Write([]byte("{\"ok\":true}\n"))
		return
	}
	rgb := make([]byte, 16*16*3)
	jpeg := []byte("\xff\xd8xgc\xff\xd9")
	response := sourceControlResponse{
		OK: true, SnapshotID: request.SnapshotID, FrameID: "camera_optical", TimestampNanoseconds: 1700000000000000000,
		Width: 16, Height: 16, PixelFormat: "rgb8", JPEGBytes: len(jpeg), RGBBytes: len(rgb),
		CameraMatrix: []float64{5, 0, 8, 0, 5, 8, 0, 0, 1}, Distortion: []float64{0, 0, 0, 0, 0},
	}
	encoded, _ := json.Marshal(response)
	_, _ = connection.Write(append(encoded, '\n'))
	_, _ = connection.Write(jpeg)
	_, _ = connection.Write(rgb)
}

func (control *captureControl) waitFor(t *testing.T, occurrences int, predicate func(sourceControlRequest) bool) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	found := 0
	for found < occurrences {
		select {
		case request := <-control.requests:
			if predicate(request) {
				found++
			}
		case <-deadline:
			t.Fatalf("capture control did not receive %d matching request(s)", occurrences)
		}
	}
}

func (control *captureControl) expectNoMatch(
	t *testing.T,
	duration time.Duration,
	predicate func(sourceControlRequest) bool,
) {
	t.Helper()
	timer := time.NewTimer(duration)
	defer timer.Stop()
	for {
		select {
		case request := <-control.requests:
			if predicate(request) {
				t.Fatalf("capture control unexpectedly received matching request: %+v", request)
			}
		case <-timer.C:
			return
		}
	}
}

func (control *captureControl) close() {
	control.once.Do(func() {
		close(control.closed)
		_ = control.listener.Close()
		_ = os.Remove(control.socket)
	})
}

func eventually(t *testing.T, timeout time.Duration, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition did not become true before timeout")
}
