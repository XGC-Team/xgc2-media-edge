package mediaedge

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type recordingSegmentMuxer interface {
	WriteAccessUnit([]byte) error
	Finalize() error
	Abort()
}

type recordingMuxerFactory func(path string, fps float64, finalizeTimeout time.Duration) (recordingSegmentMuxer, error)

// ffmpegSegmentMuxer uses FFmpeg only as a Matroska muxer. The input is the
// already encoded Annex-B H264 stream and -c:v copy is mandatory: no decoder,
// encoder, quality change, or extra NVENC session is involved.
type ffmpegSegmentMuxer struct {
	command         *exec.Cmd
	stdin           io.WriteCloser
	wait            chan error
	finalizeTimeout time.Duration
	stderr          *boundedLogBuffer
	closeOnce       sync.Once
	closeErr        error
}

func newFFmpegSegmentMuxer(
	ffmpegPath string,
) recordingMuxerFactory {
	return func(path string, fps float64, finalizeTimeout time.Duration) (recordingSegmentMuxer, error) {
		arguments := []string{
			"-nostdin",
			"-hide_banner",
			"-loglevel", "error",
			"-fflags", "+genpts",
			"-f", "h264",
			"-framerate", strconv.FormatFloat(fps, 'f', -1, 64),
			"-i", "pipe:0",
			"-map", "0:v:0",
			"-c:v", "copy",
			"-an",
			"-f", "matroska",
			"-n",
			path,
		}
		command := exec.Command(ffmpegPath, arguments...)
		stderr := &boundedLogBuffer{maximum: 16 << 10}
		command.Stdout = io.Discard
		command.Stderr = stderr
		stdin, err := command.StdinPipe()
		if err != nil {
			return nil, fmt.Errorf("open FFmpeg H264 input: %w", err)
		}
		if err := command.Start(); err != nil {
			_ = stdin.Close()
			return nil, fmt.Errorf("start FFmpeg Matroska muxer: %w", err)
		}
		muxer := &ffmpegSegmentMuxer{
			command: command, stdin: stdin, wait: make(chan error, 1),
			finalizeTimeout: finalizeTimeout, stderr: stderr,
		}
		go func() {
			muxer.wait <- command.Wait()
		}()
		return muxer, nil
	}
}

func (muxer *ffmpegSegmentMuxer) WriteAccessUnit(data []byte) error {
	if muxer == nil || muxer.stdin == nil {
		return errors.New("FFmpeg Matroska muxer is closed")
	}
	if len(data) == 0 {
		return errors.New("refuse to write an empty H264 access unit")
	}
	for len(data) > 0 {
		count, err := muxer.stdin.Write(data)
		if err != nil {
			return fmt.Errorf("write H264 access unit to FFmpeg: %w%s", err, muxer.stderrSuffix())
		}
		if count <= 0 {
			return fmt.Errorf("write H264 access unit to FFmpeg: %w%s", io.ErrShortWrite, muxer.stderrSuffix())
		}
		data = data[count:]
	}
	return nil
}

func (muxer *ffmpegSegmentMuxer) Finalize() error {
	if muxer == nil {
		return nil
	}
	muxer.closeOnce.Do(func() {
		if muxer.stdin != nil {
			muxer.closeErr = muxer.stdin.Close()
			muxer.stdin = nil
		}
		timer := time.NewTimer(muxer.finalizeTimeout)
		defer timer.Stop()
		select {
		case err := <-muxer.wait:
			if err != nil {
				muxer.closeErr = fmt.Errorf("FFmpeg Matroska muxer failed: %w%s", err, muxer.stderrSuffix())
			}
		case <-timer.C:
			if muxer.command.Process != nil {
				_ = muxer.command.Process.Kill()
			}
			<-muxer.wait
			muxer.closeErr = fmt.Errorf("FFmpeg Matroska muxer did not finalize within %s%s",
				muxer.finalizeTimeout, muxer.stderrSuffix())
		}
	})
	return muxer.closeErr
}

func (muxer *ffmpegSegmentMuxer) Abort() {
	if muxer == nil {
		return
	}
	muxer.closeOnce.Do(func() {
		if muxer.stdin != nil {
			_ = muxer.stdin.Close()
			muxer.stdin = nil
		}
		if muxer.command.Process != nil {
			_ = muxer.command.Process.Kill()
		}
		select {
		case <-muxer.wait:
		case <-time.After(muxer.finalizeTimeout):
		}
	})
}

func (muxer *ffmpegSegmentMuxer) stderrSuffix() string {
	if muxer == nil || muxer.stderr == nil {
		return ""
	}
	value := strings.TrimSpace(muxer.stderr.String())
	if value == "" {
		return ""
	}
	return ": " + value
}

type boundedLogBuffer struct {
	mu      sync.Mutex
	buffer  bytes.Buffer
	maximum int
}

func (buffer *boundedLogBuffer) Write(data []byte) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	accepted := len(data)
	remaining := buffer.maximum - buffer.buffer.Len()
	if remaining > 0 {
		if len(data) > remaining {
			data = data[:remaining]
		}
		_, _ = buffer.buffer.Write(data)
	}
	return accepted, nil
}

func (buffer *boundedLogBuffer) String() string {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.buffer.String()
}

func syncAndRenameRecordingFile(partialPath string, finalPath string) error {
	file, err := os.Open(partialPath)
	if err != nil {
		return fmt.Errorf("open completed recording segment: %w", err)
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	if syncErr != nil {
		return fmt.Errorf("sync completed recording segment: %w", syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close completed recording segment: %w", closeErr)
	}
	if err := os.Rename(partialPath, finalPath); err != nil {
		return fmt.Errorf("atomically finalize recording segment: %w", err)
	}
	return syncDirectory(finalPath)
}

func syncDirectory(childPath string) error {
	directory, err := os.Open(filepath.Dir(childPath))
	if err != nil {
		return fmt.Errorf("open recording directory for sync: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync recording directory: %w", err)
	}
	return nil
}
