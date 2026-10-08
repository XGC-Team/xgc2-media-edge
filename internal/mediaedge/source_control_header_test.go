package mediaedge

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestCameraMultipartMetadataBoundAndExactBinary(t *testing.T) {
	jpeg := bytes.Repeat([]byte{0xff, 0xd8, 0, '\n'}, 20000)
	rgb := bytes.Repeat([]byte{0, '\n', 0xff}, 30000)
	for _, padding := range []int{128, maximumControlHeaderBytes + 1} {
		t.Run(fmt.Sprint(padding), func(t *testing.T) {
			metadata := map[string]any{"ok": true, "jpegBytes": len(jpeg), "rgbBytes": len(rgb), "padding": strings.Repeat(" ", padding)}
			contentType, wire := cameraMultipartFixture(metadata, jpeg, rgb)
			_, gotJPEG, gotRGB, err := readCameraMultipart(contentType, bytes.NewReader(wire), true)
			if padding > maximumControlHeaderBytes {
				if err == nil || !strings.Contains(err.Error(), "metadata is too large") {
					t.Fatalf("unbounded metadata: %v", err)
				}
				return
			}
			if err != nil || !bytes.Equal(gotJPEG, jpeg) || !bytes.Equal(gotRGB, rgb) {
				t.Fatalf("binary changed JPEG=%d RGB=%d err=%v", len(gotJPEG), len(gotRGB), err)
			}
		})
	}
}
func TestCameraMultipartRejectsTruncationAndSizeMismatch(t *testing.T) {
	metadata := sourceControlResponse{OK: true, JPEGBytes: 3, RGBBytes: 2}
	for _, fixture := range []struct{ jpeg, rgb []byte }{{[]byte{1, 2}, []byte{3, 4}}, {[]byte{1, 2, 3, 4}, []byte{3, 4}}, {[]byte{1, 2, 3}, nil}} {
		contentType, wire := cameraMultipartFixture(metadata, fixture.jpeg, fixture.rgb)
		if _, _, _, err := readCameraMultipart(contentType, bytes.NewReader(wire), true); err == nil {
			t.Fatal("invalid frame accepted")
		}
	}
}
