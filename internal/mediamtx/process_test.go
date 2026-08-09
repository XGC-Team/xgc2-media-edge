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
	if err := process.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "exited before readiness") {
		t.Fatalf("early exit error = %v", err)
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
