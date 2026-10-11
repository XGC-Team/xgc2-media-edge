package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lxk36/xgc2-media-edge/internal/mediaedge"
)

func TestResolveSourcesRequiresConfigPath(t *testing.T) {
	if _, err := resolveSources(" \t\n"); err == nil ||
		!strings.Contains(err.Error(), "--sources-config is required") {
		t.Fatalf("required config error = %v", err)
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

	sources, err := resolveSources(path)
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

func TestSourcesConfigRejectsUnknownConfiguration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sources.json")
	if err := os.WriteFile(path, []byte(`{"sources":[{"id":"front","unknown":true}]}`), 0o600); err != nil {
		t.Fatalf("write sources config: %v", err)
	}
	if _, err := resolveSources(path); err == nil ||
		!strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown field error = %v", err)
	}
}

func TestSourcesConfigRejectsEmptyRosterAndTrailingDocument(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sources.json")
	if err := os.WriteFile(path, []byte(`{"sources":[]}`), 0o600); err != nil {
		t.Fatalf("write empty sources config: %v", err)
	}
	if _, err := resolveSources(path); err == nil ||
		!strings.Contains(err.Error(), "at least one source") {
		t.Fatalf("empty roster error = %v", err)
	}
	if err := os.WriteFile(path, []byte(`{"sources":[{"id":"front"}]} {}`), 0o600); err != nil {
		t.Fatalf("write trailing sources config: %v", err)
	}
	if _, err := resolveSources(path); err == nil ||
		!strings.Contains(err.Error(), "exactly one JSON document") {
		t.Fatalf("trailing document error = %v", err)
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

func TestCheckedInExampleRostersAreValid(t *testing.T) {
	templates, err := filepath.Abs(filepath.Join("..", "..", "pipelines"))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"two-sources.json", "pipeline-sources.json"} {
		t.Run(name, func(t *testing.T) {
			sources, err := resolveSources(filepath.Join("..", "..", "examples", name))
			if err != nil {
				t.Fatal(err)
			}
			_, err = mediaedge.NewMediaMTX(mediaedge.Config{
				ControlAddress: "127.0.0.1:18090", PipelineTemplatesDir: templates, PipelineRuntimeDir: "/run/xgc2/media-edge/pipelines",
				Sources: sources,
			}, mediaedge.MediaMTXSettings{})
			if err != nil {
				t.Fatalf("example roster %s: %v", name, err)
			}
		})
	}
}
