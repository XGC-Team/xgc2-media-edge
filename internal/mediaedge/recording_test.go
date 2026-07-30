package mediaedge

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pion/rtp"
)

func TestRecordingSegmentsOnlyAtIDRAndRecoversAfterDiscontinuity(t *testing.T) {
	item := newUnitRecording(t, 100*time.Millisecond, 32)
	go item.run(10 * time.Second)

	now := time.Now()
	item.enqueue(recordingRTPPacket(1, 0, true, stapAPayload(
		[]byte{0x67, 0x42, 0xe0, 0x1f},
		[]byte{0x68, 0xce, 0x06},
		[]byte{0x65, 0x01},
	)), now)
	item.enqueue(recordingRTPPacket(2, 4_500, true, []byte{0x41, 0x02}), now.Add(50*time.Millisecond))

	// The explicit epoch change represents either queue overflow or a source
	// restart hidden by RTP continuity rewriting. The following P-frame must
	// not be written; recording resumes only at the next IDR.
	item.markDiscontinuity()
	item.enqueue(recordingRTPPacket(3, 9_000, true, []byte{0x41, 0x03}), now.Add(100*time.Millisecond))
	item.enqueue(recordingRTPPacket(4, 18_000, true, stapAPayload(
		[]byte{0x67, 0x42, 0xe0, 0x1f},
		[]byte{0x68, 0xce, 0x06},
		[]byte{0x65, 0x04},
	)), now.Add(200*time.Millisecond))
	item.enqueue(recordingRTPPacket(5, 22_500, true, []byte{0x41, 0x05}), now.Add(250*time.Millisecond))

	item.requestStop("")
	if err := item.wait(context.Background()); err != nil {
		t.Fatalf("wait for recording: %v", err)
	}
	status := item.status()
	if status.State != RecordingComplete || status.FailureReason != "" {
		t.Fatalf("recording status = %+v", status)
	}
	if status.Discontinuities != 1 {
		t.Fatalf("discontinuities = %d, want 1", status.Discontinuities)
	}
	if status.AccessUnitsWritten != 4 {
		t.Fatalf("written access units = %d, want 4 (P-frame after discontinuity must be dropped)",
			status.AccessUnitsWritten)
	}
	if status.AccessUnitsDiscarded != 1 {
		t.Fatalf("discarded access units = %d, want 1", status.AccessUnitsDiscarded)
	}
	if len(status.Segments) != 2 {
		t.Fatalf("segments = %+v, want two independently decodable segments", status.Segments)
	}
	if status.Segments[0].EndReason != "queue-overflow-or-source-restart" ||
		status.Segments[1].EndReason != "recording-stopped" {
		t.Fatalf("segment end reasons = %+v", status.Segments)
	}
	for _, segment := range status.Segments {
		path := filepath.Join(item.root, filepath.FromSlash(segment.Path))
		if info, err := os.Stat(path); err != nil || info.Size() == 0 {
			t.Fatalf("finalized segment %q is unavailable: info=%v err=%v", path, info, err)
		}
		if _, err := os.Stat(path + ".part"); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("partial segment remained after finalize: %v", err)
		}
		indexPath := filepath.Join(item.root, filepath.FromSlash(segment.FrameIndexPath))
		index, err := os.ReadFile(indexPath)
		if err != nil {
			t.Fatalf("read finalized frame index: %v", err)
		}
		if entries := uint64(bytes.Count(index, []byte{'\n'})); entries != segment.AccessUnits {
			t.Fatalf("frame index entries = %d, want %d", entries, segment.AccessUnits)
		}
		if _, err := os.Stat(indexPath + ".part"); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("partial frame index remained after finalize: %v", err)
		}
	}
	manifestPath := filepath.Join(item.path, recordingManifestName)
	content, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var persisted RecordingManifest
	if err := json.Unmarshal(content, &persisted); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if persisted.State != RecordingComplete || len(persisted.Segments) != 2 {
		t.Fatalf("persisted manifest = %+v", persisted)
	}
}

func TestRecordingQueueOverflowIsBoundedAndNonBlocking(t *testing.T) {
	item := newUnitRecording(t, time.Second, 1)
	started := time.Now()
	for sequence := uint16(1); sequence <= 10_000; sequence++ {
		item.enqueue(
			recordingRTPPacket(sequence, uint32(sequence)*3_000, true, []byte{0x41, 0x01}),
			time.Now(),
		)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("non-blocking recorder enqueue took %s", elapsed)
	}
	status := item.status()
	if status.Queue.DepthPackets != 1 || status.Queue.OverflowPackets == 0 {
		t.Fatalf("bounded queue status = %+v", status.Queue)
	}
	if status.PacketsObserved != 10_000 || status.PacketsQueued != 1 {
		t.Fatalf("packet counters = observed=%d queued=%d", status.PacketsObserved, status.PacketsQueued)
	}
	item.requestStop("")
}

func TestRecordingIsSourceConsumerWithoutViewer(t *testing.T) {
	capture := newCaptureControl(t)
	defer capture.close()
	server := newRecordingTestServer(t, capture)
	defer server.Close()

	status, err := server.StartRecording(context.Background(), "camera", StartRecordingRequest{
		DurationSeconds: 30,
	})
	if err != nil {
		t.Fatalf("start recording: %v", err)
	}
	if _, err := server.StartRecording(context.Background(), "camera", StartRecordingRequest{
		DurationSeconds: 30,
	}); !errors.Is(err, ErrRecordingConflict) {
		t.Fatalf("concurrent source recording error = %v", err)
	}
	capture.waitFor(t, 1, func(request sourceControlRequest) bool {
		return request.Operation == "set-active" && request.Active != nil && *request.Active
	})
	capture.waitFor(t, 1, func(request sourceControlRequest) bool {
		return request.Operation == "request-keyframe"
	})
	sourceStatus := server.SourceStatuses()[0]
	if !sourceStatus.Active || sourceStatus.Consumers != 1 ||
		sourceStatus.Viewers != 0 || sourceStatus.RecordingID != status.RecordingID {
		t.Fatalf("source did not count recording consumer: %+v", sourceStatus)
	}

	sendRecordingRTP(t, server.RTPAddress("camera"), recordingRTPPacket(
		10,
		90_000,
		true,
		stapAPayload(
			[]byte{0x67, 0x42, 0xe0, 0x1f},
			[]byte{0x68, 0xce, 0x06},
			[]byte{0x65, 0x01},
		),
	))
	eventually(t, 5*time.Second, func() bool {
		current, found := server.Recording(status.RecordingID)
		return found && current.AccessUnitsWritten == 1
	})
	stopped, err := server.StopRecording(context.Background(), status.RecordingID)
	if err != nil {
		t.Fatalf("stop recording: %v", err)
	}
	if stopped.State != RecordingComplete || len(stopped.Segments) != 1 {
		t.Fatalf("stopped recording = %+v", stopped)
	}
	eventually(t, 5*time.Second, func() bool {
		return !server.SourceStatuses()[0].Active
	})
}

func TestRecordingAndWebRTCViewerShareSourceLifecycle(t *testing.T) {
	capture := newCaptureControl(t)
	defer capture.close()
	server := newRecordingTestServer(t, capture)
	defer server.Close()

	recordingStatus, err := server.StartRecording(context.Background(), "camera", StartRecordingRequest{
		DurationSeconds: 30,
	})
	if err != nil {
		t.Fatalf("start recording: %v", err)
	}
	browser, answer := openConnectedBrowserSession(t, server)
	defer browser.Close()
	eventually(t, 5*time.Second, func() bool {
		status := server.SourceStatuses()[0]
		return status.Active && status.Consumers == 2 && status.Viewers == 1 &&
			status.RecordingID == recordingStatus.RecordingID
	})

	sendRecordingRTP(t, server.RTPAddress("camera"), recordingRTPPacket(
		30,
		180_000,
		true,
		stapAPayload(
			[]byte{0x67, 0x42, 0xe0, 0x1f},
			[]byte{0x68, 0xce, 0x06},
			[]byte{0x65, 0x01},
		),
	))
	eventually(t, 5*time.Second, func() bool {
		current, found := server.Recording(recordingStatus.RecordingID)
		return found && current.AccessUnitsWritten == 1
	})
	if _, err := server.StopRecording(context.Background(), recordingStatus.RecordingID); err != nil {
		t.Fatalf("stop recording: %v", err)
	}
	status := server.SourceStatuses()[0]
	if !status.Active || status.Consumers != 1 || status.Viewers != 1 || status.RecordingID != "" {
		t.Fatalf("stopping recording disturbed viewer lifecycle: %+v", status)
	}
	if !server.CloseSession(answer.SessionID) {
		t.Fatal("close WebRTC viewer")
	}
	eventually(t, 5*time.Second, func() bool {
		return !server.SourceStatuses()[0].Active
	})
}

func TestServerCloseFinalizesActiveRecording(t *testing.T) {
	capture := newCaptureControl(t)
	defer capture.close()
	server := newRecordingTestServer(t, capture)
	started, err := server.StartRecording(context.Background(), "camera", StartRecordingRequest{
		DurationSeconds: 30,
	})
	if err != nil {
		t.Fatalf("start recording: %v", err)
	}
	sendRecordingRTP(t, server.RTPAddress("camera"), recordingRTPPacket(
		50,
		360_000,
		true,
		stapAPayload(
			[]byte{0x67, 0x42, 0xe0, 0x1f},
			[]byte{0x68, 0xce, 0x06},
			[]byte{0x65, 0x01},
		),
	))
	eventually(t, 5*time.Second, func() bool {
		current, found := server.Recording(started.RecordingID)
		return found && current.AccessUnitsWritten == 1
	})
	if err := server.Close(); err != nil {
		t.Fatalf("close server with recording: %v", err)
	}
	finalized, found := server.Recording(started.RecordingID)
	if !found || finalized.State != RecordingComplete || len(finalized.Segments) != 1 {
		t.Fatalf("shutdown-finalized recording = %+v found=%v", finalized, found)
	}
}

func TestSlowRecordingWriterDoesNotBlockRTPReceiveLoop(t *testing.T) {
	capture := newCaptureControl(t)
	defer capture.close()
	server := newRecordingTestServer(t, capture)
	defer server.Close()

	writerEntered := make(chan struct{})
	releaseWriter := make(chan struct{})
	var blockOnce sync.Once
	server.recordingMuxerFactory = func(
		path string,
		_ float64,
		_ time.Duration,
	) (recordingSegmentMuxer, error) {
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o640)
		if err != nil {
			return nil, err
		}
		return &blockingTestMuxer{
			testFileMuxer: testFileMuxer{file: file},
			block: func() {
				blockOnce.Do(func() {
					close(writerEntered)
					<-releaseWriter
				})
			},
		}, nil
	}
	started, err := server.StartRecording(context.Background(), "camera", StartRecordingRequest{
		DurationSeconds: 30,
	})
	if err != nil {
		t.Fatalf("start recording: %v", err)
	}
	sendRecordingRTP(t, server.RTPAddress("camera"), recordingRTPPacket(
		100,
		450_000,
		true,
		stapAPayload(
			[]byte{0x67, 0x42, 0xe0, 0x1f},
			[]byte{0x68, 0xce, 0x06},
			[]byte{0x65, 0x01},
		),
	))
	select {
	case <-writerEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("recording writer did not enter blocking muxer")
	}

	startedSending := time.Now()
	packets := make([]*rtp.Packet, 0, 200)
	for index := 1; index <= 200; index++ {
		packets = append(packets, recordingRTPPacket(
			uint16(100+index),
			450_000+uint32(index)*4_500,
			true,
			[]byte{0x41, byte(index)},
		))
	}
	sendRecordingRTPBatch(t, server.RTPAddress("camera"), packets)
	if elapsed := time.Since(startedSending); elapsed > time.Second {
		t.Fatalf("RTP receive path was blocked by recorder for %s", elapsed)
	}
	eventually(t, 5*time.Second, func() bool {
		return server.SourceStatuses()[0].PacketsReceived == 201
	})
	current, found := server.Recording(started.RecordingID)
	if !found || current.Queue.OverflowPackets == 0 {
		t.Fatalf("slow-writer recording status = %+v found=%v", current, found)
	}

	close(releaseWriter)
	eventually(t, 5*time.Second, func() bool {
		current, found := server.Recording(started.RecordingID)
		return found && current.Queue.DepthPackets == 0
	})
	sendRecordingRTP(t, server.RTPAddress("camera"), recordingRTPPacket(
		301,
		1_350_000,
		true,
		stapAPayload(
			[]byte{0x67, 0x42, 0xe0, 0x1f},
			[]byte{0x68, 0xce, 0x06},
			[]byte{0x65, 0x02},
		),
	))
	eventually(t, 5*time.Second, func() bool {
		current, found := server.Recording(started.RecordingID)
		return found && current.Discontinuities > 0 && current.AccessUnitsWritten > 1
	})
	if _, err := server.StopRecording(context.Background(), started.RecordingID); err != nil {
		t.Fatalf("stop slow-writer recording: %v", err)
	}
}

func TestRecordingCapacityAdmissionUsesPeakBitrateAndReserve(t *testing.T) {
	root := t.TempDir()
	config := RecordingConfig{
		Root: root, MaxBitrateBitsPerSecond: 10_000_000_000,
		CapacitySafetyFactor: 10, MinimumFreeBytes: math.MaxUint64,
	}
	err := ensureRecordingCapacity(config, 7*24*time.Hour)
	if !errors.Is(err, ErrInsufficientCapacity) {
		t.Fatalf("oversized recording capacity error = %v", err)
	}
}

func TestRecordingPathsCannotEscapeConfiguredRoot(t *testing.T) {
	root := t.TempDir()
	for _, elements := range [][]string{
		{"..", "outside.mkv"},
		{"recording", "../outside.mkv"},
		{"/tmp/outside.mkv"},
		{""},
	} {
		if path, err := recordingPathWithinRoot(root, elements...); err == nil {
			t.Fatalf("unsafe recording path %q was accepted as %q", elements, path)
		}
	}
	path, err := recordingPathWithinRoot(root, "recording", "segments", "segment-000001.mkv")
	if err != nil || !strings.HasPrefix(path, root+string(os.PathSeparator)) {
		t.Fatalf("safe recording path = %q error=%v", path, err)
	}
}

func TestInterruptedRecordingIsRecoveredAsFailedHistory(t *testing.T) {
	root := t.TempDir()
	const recordingID = "abcdefabcdefabcdefabcdefabcdefab"
	if _, err := createRecordingDirectory(root, recordingID); err != nil {
		t.Fatalf("create interrupted recording: %v", err)
	}
	manifest := RecordingManifest{
		SchemaVersion: recordingManifestSchema, RecordingID: recordingID,
		SourceID: "camera", State: RecordingActive, CreatedAt: time.Now().Add(-time.Minute).UTC(),
		Segments: []RecordingSegment{},
	}
	if err := writeRecordingManifest(root, manifest); err != nil {
		t.Fatalf("write interrupted manifest: %v", err)
	}
	server, err := New(Config{
		ControlAddress: "127.0.0.1:0",
		Recording: RecordingConfig{
			Root: root, FFmpegPath: "/bin/true",
			MaxBitrateBitsPerSecond: 1_000_000,
		},
		Sources: []SourceConfig{{
			ID: "camera", RTPListenAddress: "127.0.0.1:5004",
			ControlSocket: "/tmp/camera.sock",
		}},
	})
	if err != nil {
		t.Fatalf("create recovery server: %v", err)
	}
	defer server.cancelLifecycle()
	if err := server.prepareRecording(); err != nil {
		t.Fatalf("prepare recording recovery: %v", err)
	}
	recovered, found := server.Recording(recordingID)
	if !found || recovered.State != RecordingFailed || recovered.StoppedAt == nil ||
		!strings.Contains(recovered.FailureReason, "restarted before recording finalized") {
		t.Fatalf("recovered recording = %+v found=%v", recovered, found)
	}
}

func TestEnabledRecordingRequiresAvailableFFmpegMuxer(t *testing.T) {
	server, err := New(Config{
		ControlAddress: "127.0.0.1:0",
		Recording: RecordingConfig{
			Root: t.TempDir(), FFmpegPath: "/definitely/missing/xgc-ffmpeg",
			MaxBitrateBitsPerSecond: 1_000_000,
		},
		Sources: []SourceConfig{{
			ID: "camera", RTPListenAddress: "127.0.0.1:5004",
			ControlSocket: "/tmp/camera.sock",
		}},
	})
	if err != nil {
		t.Fatalf("create recording config: %v", err)
	}
	defer server.cancelLifecycle()
	err = server.prepareRecording()
	if err == nil || !strings.Contains(err.Error(), "requires FFmpeg") {
		t.Fatalf("missing FFmpeg error = %v", err)
	}
}

type testFileMuxer struct {
	file *os.File
}

type blockingTestMuxer struct {
	testFileMuxer
	block func()
}

func (muxer *blockingTestMuxer) WriteAccessUnit(data []byte) error {
	muxer.block()
	return muxer.testFileMuxer.WriteAccessUnit(data)
}

func (muxer *testFileMuxer) WriteAccessUnit(data []byte) error {
	_, err := muxer.file.Write(data)
	return err
}

func (muxer *testFileMuxer) Finalize() error {
	return muxer.file.Close()
}

func (muxer *testFileMuxer) Abort() {
	_ = muxer.file.Close()
}

func testRecordingMuxerFactory(
	path string,
	_ float64,
	_ time.Duration,
) (recordingSegmentMuxer, error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o640)
	if err != nil {
		return nil, err
	}
	return &testFileMuxer{file: file}, nil
}

func newUnitRecording(t *testing.T, segmentDuration time.Duration, queuePackets int) *recording {
	t.Helper()
	root := t.TempDir()
	const recordingID = "0123456789abcdef0123456789abcdef"
	path, err := createRecordingDirectory(root, recordingID)
	if err != nil {
		t.Fatalf("create unit recording directory: %v", err)
	}
	server := &Server{
		config:  Config{SessionGracePeriod: time.Second},
		sources: make(map[string]*source), sessions: make(map[string]*session),
		recordings: make(map[string]*recording), recordingHistory: make(map[string]RecordingManifest),
		lifecycleContext: context.Background(), closed: make(chan struct{}),
	}
	source := &source{
		server:   server,
		config:   SourceConfig{ID: "camera", Width: 16, Height: 16, FPS: 30, FrameID: "camera_optical"},
		sessions: make(map[string]struct{}), snapshots: make(map[string]Snapshot),
	}
	item := &recording{
		server: server, source: source, root: root, path: path,
		config: RecordingConfig{
			Root: root, QueuePackets: queuePackets, SegmentDuration: segmentDuration,
			FinalizeTimeout: time.Second, KeyframeTimeout: 2 * time.Second,
		},
		muxerFactory: testRecordingMuxerFactory,
		accepting:    true, queue: make(chan recordingPacket, queuePackets), done: make(chan struct{}),
		manifest: RecordingManifest{
			SchemaVersion: recordingManifestSchema, RecordingID: recordingID, SourceID: "camera",
			State: RecordingWaiting, CreatedAt: time.Now().UTC(), RequestedDurationSeconds: 10,
			Codec:    RecordingCodec{Name: sourceCodec, Container: recordingContainer, FPS: 30, StreamCopy: true},
			Queue:    RecordingQueueStatus{CapacityPackets: queuePackets},
			Segments: []RecordingSegment{},
		},
	}
	source.recording = item
	server.sources["camera"] = source
	server.recordings[recordingID] = item
	if err := item.persistManifest(); err != nil {
		t.Fatalf("persist initial unit manifest: %v", err)
	}
	return item
}

func newRecordingTestServer(t *testing.T, capture *captureControl) *Server {
	t.Helper()
	rtpAddress := availableLoopbackRTPAddress(t)
	capture.setRTPDestination(t, rtpAddress)
	server, err := New(Config{
		ControlAddress:       "127.0.0.1:0",
		SessionGracePeriod:   5 * time.Millisecond,
		SessionGatherTimeout: 3 * time.Second,
		Recording: RecordingConfig{
			Root: t.TempDir(), FFmpegPath: "/bin/true",
			MaxBitrateBitsPerSecond: 1_000_000,
			QueuePackets:            64, SegmentDuration: time.Second, MaxDuration: time.Minute,
			FinalizeTimeout: time.Second, KeyframeTimeout: 10 * time.Second,
			MinimumFreeBytes: 1, CapacitySafetyFactor: 1,
		},
		Sources: []SourceConfig{{
			ID: "camera", RTPListenAddress: rtpAddress, ControlSocket: capture.socket,
			Width: 16, Height: 16, FPS: 20, FrameID: "camera_optical",
		}},
	})
	if err != nil {
		t.Fatalf("create recording media edge: %v", err)
	}
	if err := server.Start(); err != nil {
		t.Fatalf("start recording media edge: %v", err)
	}
	server.recordingMuxerFactory = testRecordingMuxerFactory
	source := server.source("camera")
	source.mu.Lock()
	source.stallTimeout = time.Hour
	source.mu.Unlock()
	return server
}

func recordingRTPPacket(sequence uint16, timestamp uint32, marker bool, payload []byte) *rtp.Packet {
	return &rtp.Packet{
		Header: rtp.Header{
			Version: 2, PayloadType: 96, SequenceNumber: sequence,
			Timestamp: timestamp, Marker: marker,
		},
		Payload: append([]byte(nil), payload...),
	}
}

func sendRecordingRTP(t *testing.T, address string, packet *rtp.Packet) {
	t.Helper()
	sendRecordingRTPBatch(t, address, []*rtp.Packet{packet})
}

func sendRecordingRTPBatch(t *testing.T, address string, packets []*rtp.Packet) {
	t.Helper()
	connection, err := net.Dial("udp", address)
	if err != nil {
		t.Fatalf("dial recording RTP ingress: %v", err)
	}
	defer connection.Close()
	for _, packet := range packets {
		encoded, err := packet.Marshal()
		if err != nil {
			t.Fatalf("marshal recording RTP: %v", err)
		}
		if _, err := connection.Write(encoded); err != nil {
			t.Fatalf("send recording RTP: %v", err)
		}
	}
}
