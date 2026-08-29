package mediamtx

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestRenderPinsMinimalMediaMTXSurface(t *testing.T) {
	content, err := Render(Config{
		APIAddress: "127.0.0.1:19997", WHEPAddress: "127.0.0.1:18889",
		ICEUDPAddress: "0.0.0.0:18189", IPsFromInterfaces: true,
		AllowedOrigins:  []string{"http://127.0.0.1:5173", "http://127.0.0.1:5173"},
		AdditionalHosts: []string{"192.168.1.20"},
		Paths: []Path{{Name: "front", RTPAddress: "127.0.0.1:5004",
			RecordPath: "/var/lib/xgc/media/front/%Y-%m-%d_%H-%M-%S-%f"}},
	})
	if err != nil {
		t.Fatalf("render MediaMTX config: %v", err)
	}
	var rendered map[string]any
	if err := json.Unmarshal(content, &rendered); err != nil {
		t.Fatalf("rendered config is not JSON/YAML: %v", err)
	}
	for _, disabled := range []string{"rtsp", "rtmp", "hls", "srt", "moq", "metrics", "pprof", "playback"} {
		if value, found := rendered[disabled]; !found || value != false {
			t.Fatalf("%s = %#v found=%v, want explicitly disabled", disabled, value, found)
		}
	}
	paths := rendered["paths"].(map[string]any)
	front := paths["front"].(map[string]any)
	if front["source"] != "udp+rtp://127.0.0.1:5004" || front["record"] != false ||
		!strings.Contains(front["rtpSDP"].(string), "a=rtpmap:96 H264/90000") {
		t.Fatalf("rendered front path = %#v", front)
	}
	if rendered["webrtcLocalUDPAddress"] != "0.0.0.0:18189" || rendered["apiAddress"] != "127.0.0.1:19997" {
		t.Fatalf("rendered boundaries = %#v", rendered)
	}
	if value, found := rendered["udpReadBufferSize"]; !found || value != float64(8<<20) {
		t.Fatalf("udpReadBufferSize = %#v found=%v, want 8 MiB", value, found)
	}
	if ice, ok := rendered["webrtcICEServers2"].([]any); !ok || ice == nil || len(ice) != 0 {
		t.Fatalf("empty ICE server list = %#v, want an empty array", rendered["webrtcICEServers2"])
	}
}

func TestRenderRejectsUnsafeOrAmbiguousBoundaries(t *testing.T) {
	valid := Config{
		APIAddress: "127.0.0.1:19997", WHEPAddress: "127.0.0.1:18889",
		ICEUDPAddress: "0.0.0.0:18189",
		Paths:         []Path{{Name: "front", RTPAddress: "127.0.0.1:5004"}},
	}
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"remote API", func(value *Config) { value.APIAddress = "0.0.0.0:19997" }, "loopback"},
		{"remote WHEP", func(value *Config) { value.WHEPAddress = "0.0.0.0:18889" }, "loopback"},
		{"remote ingest", func(value *Config) { value.Paths[0].RTPAddress = "192.168.1.2:5004" }, "loopback"},
		{"path injection", func(value *Config) { value.Paths[0].Name = "front/../../x" }, "invalid"},
		{"relative record", func(value *Config) { value.Paths[0].RecordPath = "recordings/front" }, "absolute"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := valid
			candidate.Paths = append([]Path(nil), valid.Paths...)
			test.mutate(&candidate)
			if _, err := Render(candidate); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Render error = %v, want %q", err, test.want)
			}
		})
	}
}
