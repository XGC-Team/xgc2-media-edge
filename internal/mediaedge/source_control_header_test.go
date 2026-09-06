package mediaedge

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSourceControlHeaderBoundaryAndBufferedPayload(t *testing.T) {
	jpeg := bytes.Repeat([]byte{0xff, 0xd8, 0, '\n'}, 20000)
	rgb := bytes.Repeat([]byte{0, '\n', 0xff}, 30000)
	for _, headerSize := range []int{128, maximumControlHeaderBytes, maximumControlHeaderBytes + 1} {
		t.Run(fmt.Sprint(headerSize), func(t *testing.T) {
			header := fmt.Sprintf(`{"ok":true,"jpegBytes":%d,"rgbBytes":%d}`, len(jpeg), len(rgb))
			header += strings.Repeat(" ", headerSize-len(header)-1) + "\n"
			wire := append(append([]byte(header), jpeg...), rgb...)
			socket := serveControlHeader(t, wire, false)
			_, gotJPEG, gotRGB, err := callSourceControl(context.Background(), socket, sourceControlRequest{Operation: "snapshot"})
			if headerSize > maximumControlHeaderBytes {
				if err == nil || !strings.Contains(err.Error(), "header is too large") {
					t.Fatalf("oversized header: %v", err)
				}
				return
			}
			if err != nil || !bytes.Equal(gotJPEG, jpeg) || !bytes.Equal(gotRGB, rgb) {
				t.Fatalf("payload changed: JPEG=%d RGB=%d err=%v", len(gotJPEG), len(gotRGB), err)
			}
		})
	}
}

func TestSourceControlUnterminatedHeaderRejectsBeforeTimeout(t *testing.T) {
	// The peer sends only limit+1 bytes and keeps the socket open. A reader that
	// waits for newline/EOF reaches the deadline instead of rejecting the size.
	socket := serveControlHeader(t, bytes.Repeat([]byte{'x'}, maximumControlHeaderBytes+1), true)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _, _, err := callSourceControl(ctx, socket, sourceControlRequest{Operation: "describe"})
	if err == nil || !strings.Contains(err.Error(), "header is too large") {
		t.Fatalf("expected immediate size rejection, got %v", err)
	}
	if ctx.Err() != nil {
		t.Fatalf("header bound waited for transaction deadline: %v", ctx.Err())
	}
}

func serveControlHeader(t *testing.T, wire []byte, holdOpen bool) string {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "source.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	stop, done := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		close(stop)
		_ = listener.Close()
		<-done
	})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := bufio.NewReader(conn).ReadString('\n'); err != nil {
			return
		}
		_, _ = io.Copy(conn, bytes.NewReader(wire))
		if holdOpen {
			<-stop
		}
	}()
	return socket
}
