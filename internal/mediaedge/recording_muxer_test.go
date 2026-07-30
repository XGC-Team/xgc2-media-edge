package mediaedge

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestFFmpegSegmentMuxerStreamCopiesValidH264ToMatroska(t *testing.T) {
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		t.Skip("optional FFmpeg dependency is not installed")
	}
	directory := t.TempDir()
	inputPath := filepath.Join(directory, "input.h264")
	generate := exec.Command(
		ffmpeg,
		"-nostdin", "-hide_banner", "-loglevel", "error",
		"-f", "lavfi", "-i", "testsrc=size=64x64:rate=10",
		"-frames:v", "3",
		"-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
		"-f", "h264", "-y", inputPath,
	)
	if output, err := generate.CombinedOutput(); err != nil {
		t.Skipf("installed FFmpeg cannot generate the H264 test fixture: %v: %s", err, output)
	}
	input, err := os.ReadFile(inputPath)
	if err != nil {
		t.Fatalf("read generated H264: %v", err)
	}
	outputPath := filepath.Join(directory, "segment.mkv.part")
	muxer, err := newFFmpegSegmentMuxer(ffmpeg)(outputPath, 10, 5*time.Second)
	if err != nil {
		t.Fatalf("start FFmpeg stream-copy muxer: %v", err)
	}
	if err := muxer.WriteAccessUnit(input); err != nil {
		muxer.Abort()
		t.Fatalf("write generated H264: %v", err)
	}
	if err := muxer.Finalize(); err != nil {
		t.Fatalf("finalize generated Matroska: %v", err)
	}
	output, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read generated Matroska: %v", err)
	}
	if len(output) < 4 || !bytes.Equal(output[:4], []byte{0x1a, 0x45, 0xdf, 0xa3}) {
		t.Fatalf("FFmpeg output is not Matroska: %x", output[:min(len(output), 16)])
	}
}
