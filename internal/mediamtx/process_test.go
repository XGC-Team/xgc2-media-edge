package mediamtx

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestProcessRequiresPinnedVersionAndStopsGracefully(t *testing.T) {
	runtimeDir := t.TempDir()
	executable := writeProcessFixture(t, `#!/bin/sh
if [ "$1" = "--version" ]; then
  echo v1.20.0
  exit 0
fi
trap 'exit 0' INT TERM
while :; do sleep 1; done
`)
	var probes atomic.Int32
	process, err := NewProcess(ProcessConfig{
		Executable: executable, RuntimeDir: runtimeDir, Configuration: []byte(`{"api":true}`),
		Readiness: func(context.Context) error {
			if probes.Add(1) < 2 {
				return errors.New("not ready")
			}
			return nil
		},
		StopTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("create process: %v", err)
	}
	if err := process.Start(context.Background()); err != nil {
		t.Fatalf("start process: %v", err)
	}
	configuration := filepath.Join(runtimeDir, "mediamtx.json")
	info, err := os.Stat(configuration)
	if err != nil {
		t.Fatalf("stat runtime configuration: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("configuration mode = %o", info.Mode().Perm())
	}
	if err := process.Close(); err != nil {
		t.Fatalf("close process: %v", err)
	}
	if _, err := os.Stat(configuration); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("runtime configuration remained after close: %v", err)
	}
}

func TestProcessRejectsUnpinnedVersion(t *testing.T) {
	executable := writeProcessFixture(t, `#!/bin/sh
echo v1.19.0
`)
	process, err := NewProcess(ProcessConfig{
		Executable: executable, RuntimeDir: t.TempDir(), Configuration: []byte(`{}`),
		Readiness: func(context.Context) error { return nil },
	})
	if err != nil {
		t.Fatalf("create process: %v", err)
	}
	if err := process.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "want pinned") {
		t.Fatalf("unpinned process error = %v", err)
	}
}

func TestProcessSurfacesExitBeforeReadiness(t *testing.T) {
	executable := writeProcessFixture(t, `#!/bin/sh
if [ "$1" = "--version" ]; then
  echo v1.20.0
  exit 0
fi
echo "ERR unable to set read buffer size to 8388608"
exit 7
`)
	process, err := NewProcess(ProcessConfig{
		Executable: executable, RuntimeDir: t.TempDir(), Configuration: []byte(`{}`),
		Readiness:      func(context.Context) error { return errors.New("not ready") },
		StartupTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("create process: %v", err)
	}
	if err := process.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "exited before readiness") ||
		!strings.Contains(err.Error(), "unable to set read buffer size") {
		t.Fatalf("early exit error = %v", err)
	}
	_ = process.Close()
}

// A grandchild that inherits stdout keeps the log pipe open after MediaMTX
// exits. Wait must not block on it, or the startup error would never surface.
func TestProcessExitIsNotBlockedByLingeringStdoutHolder(t *testing.T) {
	executable := writeProcessFixture(t, `#!/bin/sh
if [ "$1" = "--version" ]; then
  echo v1.20.0
  exit 0
fi
sleep 5 &
echo "ERR rejected socket option"
exit 7
`)
	process, err := NewProcess(ProcessConfig{
		Executable: executable, RuntimeDir: t.TempDir(), Configuration: []byte(`{}`),
		Readiness:      func(context.Context) error { return errors.New("not ready") },
		StartupTimeout: 4 * time.Second,
	})
	if err != nil {
		t.Fatalf("create process: %v", err)
	}
	started := time.Now()
	err = process.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "exited before readiness") || !strings.Contains(err.Error(), "rejected socket option") {
		t.Fatalf("early exit error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("exit surfaced after %v, want the short wait delay", elapsed)
	}
	_ = process.Close()
}

func writeProcessFixture(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "mediamtx")
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatalf("write process fixture: %v", err)
	}
	return path
}
