package mediamtx

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

const realMediaMTXExecutableEnv = "XGC2_MEDIAMTX_TEST_EXECUTABLE"

// TestRealMediaMTXStartsInUnprivilegedProbe is compiled into the package
// readiness probe by verify_unprivileged_mediamtx.sh. Product config tests own
// the 8 MiB receive-buffer default; this probe deliberately sets the upstream
// binary to the OS default because its no-capability container cannot raise the
// host rmem_max deployment prerequisite.
func TestRealMediaMTXStartsInUnprivilegedProbe(t *testing.T) {
	executable := strings.TrimSpace(os.Getenv(realMediaMTXExecutableEnv))
	if executable == "" {
		t.Skip(realMediaMTXExecutableEnv + " is not set")
	}
	if info, err := os.Stat(executable); err != nil {
		t.Fatalf("stat real MediaMTX executable: %v", err)
	} else if info.Mode()&0o111 == 0 {
		t.Fatalf("real MediaMTX executable is not executable: %s", executable)
	}

	apiAddress := freeTCPAddress(t)
	whepAddress := freeTCPAddress(t)
	configuration, err := Render(Config{
		APIAddress:        apiAddress,
		WHEPAddress:       whepAddress,
		ICEUDPAddress:     freeUDPAddress(t),
		IPsFromInterfaces: true,
		Paths: []Path{{
			Name:       "readiness",
			RTPAddress: freeUDPAddress(t),
		}},
	})
	if err != nil {
		t.Fatalf("render real MediaMTX configuration: %v", err)
	}
	var probeConfiguration map[string]any
	if err := json.Unmarshal(configuration, &probeConfiguration); err != nil {
		t.Fatalf("decode real MediaMTX probe configuration: %v", err)
	}
	probeConfiguration["udpReadBufferSize"] = 0
	configuration, err = json.Marshal(probeConfiguration)
	if err != nil {
		t.Fatalf("encode real MediaMTX probe configuration: %v", err)
	}
	client, err := NewClient("http://"+apiAddress, "http://"+whepAddress)
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
		t.Fatalf("start real MediaMTX in unprivileged probe: %v\n%s", err, logs.String())
	}
	if err := readiness(ctx); err != nil {
		t.Fatalf("real MediaMTX lost readiness after startup: %v\n%s", err, logs.String())
	}
	if err := process.Close(); err != nil {
		t.Fatalf("stop real MediaMTX: %v\n%s", err, logs.String())
	}
}

func freeTCPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate TCP probe address: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release TCP probe address: %v", err)
	}
	return address
}

func freeUDPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate UDP probe address: %v", err)
	}
	address := listener.LocalAddr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release UDP probe address: %v", err)
	}
	return address
}
