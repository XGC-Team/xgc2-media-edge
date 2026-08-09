package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveSourcesKeepsLegacySingleSourceFlags(t *testing.T) {
	sources, err := resolveSources("", legacySourceFlags{
		ID: "front", RTPListenAddress: "127.0.0.1:5004",
		ControlSocket: "/tmp/front.sock", Width: 1280, Height: 720,
		FPS: 30, FrameID: "front_optical",
	})
	if err != nil {
		t.Fatalf("resolve legacy source: %v", err)
	}
	if len(sources) != 1 || sources[0].ID != "front" || sources[0].FPS != 30 {
		t.Fatalf("legacy source = %+v", sources)
	}
}

func TestLoadSourcesAcceptsMultipleStrictSourceEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sources.json")
	content := `{"sources":[
		{"id":"front","rtpListenAddress":"127.0.0.1:5004","controlSocket":"/tmp/front.sock","width":1280,"height":720,"fps":30,"frameId":"front_optical"},
		{"id":"world","rtpListenAddress":"127.0.0.1:5006","controlSocket":"/tmp/world.sock"}
	]}`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write sources config: %v", err)
	}

	sources, err := resolveSources(path, legacySourceFlags{})
	if err != nil {
		t.Fatalf("resolve sources config: %v", err)
	}
	if len(sources) != 2 || sources[0].ID != "front" || sources[1].ID != "world" {
		t.Fatalf("sources = %+v", sources)
	}
	if got := sourceIDs(sources); got != "front,world" {
		t.Fatalf("source IDs = %q", got)
	}
}

func TestSourcesConfigRejectsAmbiguousOrUnknownConfiguration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sources.json")
	if err := os.WriteFile(path, []byte(`{"sources":[{"id":"front","unknown":true}]}`), 0o600); err != nil {
		t.Fatalf("write sources config: %v", err)
	}
	if _, err := resolveSources(path, legacySourceFlags{}); err == nil ||
		!strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown field error = %v", err)
	}
	if _, err := resolveSources(path, legacySourceFlags{ID: "front"}); err == nil ||
		!strings.Contains(err.Error(), "cannot be combined") {
		t.Fatalf("mixed source configuration error = %v", err)
	}
}

func TestSourcesConfigRejectsOversizedDocument(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sources.json")
	content := make([]byte, maximumSourcesConfigBytes+1)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatalf("write oversized sources config: %v", err)
	}
	if _, err := loadSources(path); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized sources config error = %v", err)
	}
}
