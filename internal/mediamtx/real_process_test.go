package mediamtx

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

const realMediaMTXExecutableEnv = "XGC2_MEDIAMTX_TEST_EXECUTABLE"

// TestRealMediaMTXStartsWithOSDefaultUDPBuffer is compiled into the package
// readiness probe by verify_unprivileged_mediamtx.sh. Normal source tests skip
// it because the pinned release binary belongs to the Debian artifact, not the
// Go source tree.
func TestRealMediaMTXStartsWithOSDefaultUDPBuffer(t *testing.T) {
	executable := strings.TrimSpace(os.Getenv(realMediaMTXExecutableEnv))
	if executable == "" {
		t.Skip(realMediaMTXExecutableEnv + " is not set")
	}
	if info, err := os.Stat(executable); err != nil {
		t.Fatalf("stat real MediaMTX executable: %v", err)
	} else if info.Mode()&0o111 == 0 {
		t.Fatalf("real MediaMTX executable is not executable: %s", executable)
	}

	configuration, err := Render(Config{
		APIAddress:        "127.0.0.1:19997",
		WHEPAddress:       "127.0.0.1:18889",
		ICEUDPAddress:     "127.0.0.1:18189",
		IPsFromInterfaces: true,
		Paths: []Path{{
			Name:       "readiness",
			RTPAddress: "127.0.0.1:15004",
		}},
	})
	if err != nil {
		t.Fatalf("render real MediaMTX configuration: %v", err)
	}
	client, err := NewClient("http://127.0.0.1:19997", "http://127.0.0.1:18889")
	if err != nil {
		t.Fatalf("create real MediaMTX client: %v", err)
	}
	readiness := func(ctx context.Context) error {
		paths, err := client.Paths(ctx)
		if err != nil {
			return err
		}
		for _, path := range paths {
			if path.Name == "readiness" {
				return nil
			}
		}
		return fmt.Errorf("configured readiness path is missing")
	}

	var logs bytes.Buffer
	process, err := NewProcess(ProcessConfig{
		Executable:     executable,
		RuntimeDir:     t.TempDir(),
		Configuration:  configuration,
		Readiness:      readiness,
		Stdout:         &logs,
		Stderr:         &logs,
		StartupTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("create real MediaMTX process: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := process.Start(ctx); err != nil {
		t.Fatalf("start real MediaMTX with OS-default UDP buffer: %v\n%s", err, logs.String())
	}
	if err := readiness(ctx); err != nil {
		t.Fatalf("real MediaMTX lost readiness after startup: %v\n%s", err, logs.String())
	}
	if err := process.Close(); err != nil {
		t.Fatalf("stop real MediaMTX: %v\n%s", err, logs.String())
	}
}
