package mediaedge

import (
	"strings"
	"testing"
)

func TestAllowedOriginsAreExactHTTPOrigins(t *testing.T) {
	config, err := (Config{
		ControlAddress: "0.0.0.0:18090",
		AllowedOrigins: []string{
			" HTTPS://STATION.EXAMPLE:8443/ ",
			"https://station.example:8443",
			"http://192.0.2.10:3000",
			"http://camera.example:80",
		},
		Sources: []SourceConfig{{
			ID: "camera", RTPListenAddress: "127.0.0.1:5004",
			ControlSocket: "/tmp/camera.sock",
		}},
	}).normalized()
	if err != nil {
		t.Fatalf("normalize valid origins: %v", err)
	}
	want := []string{
		"https://station.example:8443",
		"http://192.0.2.10:3000",
		"http://camera.example",
	}
	if len(config.AllowedOrigins) != len(want) {
		t.Fatalf("normalized origins = %v, want %v", config.AllowedOrigins, want)
	}
	for index := range want {
		if config.AllowedOrigins[index] != want[index] {
			t.Fatalf("normalized origins = %v, want %v", config.AllowedOrigins, want)
		}
	}
}

func TestAllowedOriginsRejectURLsAndCredentials(t *testing.T) {
	invalid := []string{
		"",
		"*",
		"ftp://station.example",
		"http://",
		"http://user@station.example",
		"http://station.example/video",
		"http://station.example?mode=video",
		"http://station.example#video",
		"http://station.example:99999",
	}
	for _, origin := range invalid {
		t.Run(strings.ReplaceAll(origin, "/", "_"), func(t *testing.T) {
			_, err := (Config{
				ControlAddress: "127.0.0.1:18090",
				AllowedOrigins: []string{origin},
				Sources: []SourceConfig{{
					ID: "camera", RTPListenAddress: "127.0.0.1:5004",
					ControlSocket: "/tmp/camera.sock",
				}},
			}).normalized()
			if err == nil {
				t.Fatalf("invalid origin %q was accepted", origin)
			}
		})
	}
}

func TestSourceIDMustBeStable(t *testing.T) {
	for _, sourceID := range []string{"", "camera/front", `camera"`, strings.Repeat("a", 129)} {
		t.Run(strings.ReplaceAll(sourceID, "/", "_"), func(t *testing.T) {
			_, err := (SourceConfig{
				ID:               sourceID,
				RTPListenAddress: "127.0.0.1:5004",
				ControlSocket:    "/tmp/camera.sock",
			}).normalized()
			if err == nil {
				t.Fatalf("invalid source ID %q was accepted", sourceID)
			}
		})
	}
}

func TestRTPIngressRequiresFixedLoopbackPort(t *testing.T) {
	for _, address := range []string{"127.0.0.1:0", "127.0.0.1:not-a-port", "0.0.0.0:5004"} {
		t.Run(strings.ReplaceAll(address, ":", "_"), func(t *testing.T) {
			_, err := (SourceConfig{
				ID:               "camera",
				RTPListenAddress: address,
				ControlSocket:    "/tmp/camera.sock",
			}).normalized()
			if err == nil {
				t.Fatalf("invalid RTP ingress address %q was accepted", address)
			}
		})
	}
}
