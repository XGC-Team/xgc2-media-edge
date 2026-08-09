package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/lxk36/xgc2-media-edge/internal/mediaedge"
)

const maximumSourcesConfigBytes = 1 << 20

type sourcesDocument struct {
	Sources []mediaedge.SourceConfig `json:"sources"`
}

type legacySourceFlags struct {
	ID               string
	RTPListenAddress string
	ControlSocket    string
	Width            int
	Height           int
	FPS              float64
	FrameID          string
}

func resolveSources(path string, legacy legacySourceFlags) ([]mediaedge.SourceConfig, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return []mediaedge.SourceConfig{{
			ID: legacy.ID, RTPListenAddress: legacy.RTPListenAddress,
			ControlSocket: legacy.ControlSocket, Width: legacy.Width,
			Height: legacy.Height, FPS: legacy.FPS, FrameID: legacy.FrameID,
		}}, nil
	}
	if legacy.configured() {
		return nil, errors.New("--sources-config cannot be combined with single-source flags")
	}
	return loadSources(path)
}

func (legacy legacySourceFlags) configured() bool {
	return strings.TrimSpace(legacy.ID) != "" ||
		strings.TrimSpace(legacy.RTPListenAddress) != "" ||
		strings.TrimSpace(legacy.ControlSocket) != "" ||
		legacy.Width != 0 || legacy.Height != 0 || legacy.FPS != 0 ||
		strings.TrimSpace(legacy.FrameID) != ""
}

func loadSources(path string) ([]mediaedge.SourceConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open sources config: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat sources config: %w", err)
	}
	if info.Size() > maximumSourcesConfigBytes {
		return nil, fmt.Errorf("sources config exceeds %d bytes", maximumSourcesConfigBytes)
	}

	decoder := json.NewDecoder(io.LimitReader(file, maximumSourcesConfigBytes+1))
	decoder.DisallowUnknownFields()
	var document sourcesDocument
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("decode sources config: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return nil, err
	}
	if len(document.Sources) == 0 {
		return nil, errors.New("sources config must contain at least one source")
	}
	return document.Sources, nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("sources config must contain exactly one JSON document")
		}
		return fmt.Errorf("decode trailing sources config data: %w", err)
	}
	return nil
}

func sourceIDs(sources []mediaedge.SourceConfig) string {
	ids := make([]string, 0, len(sources))
	for _, source := range sources {
		ids = append(ids, source.ID)
	}
	sort.Strings(ids)
	return strings.Join(ids, ",")
}
