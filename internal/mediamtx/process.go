package mediamtx

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	defaultStartupTimeout = 10 * time.Second
	defaultStopTimeout    = 5 * time.Second
)

type ProcessConfig struct {
	Executable     string
	RuntimeDir     string
	Configuration  []byte
	Readiness      func(context.Context) error
	Stdout         io.Writer
	Stderr         io.Writer
	StartupTimeout time.Duration
	StopTimeout    time.Duration
}

// Process supervises exactly one pinned MediaMTX child. Restart policy belongs
// to the Agent/Session process supervisor; silently forking a replacement here
// would detach failures from Experiment lifecycle truth.
type Process struct {
	config ProcessConfig

	mu         sync.Mutex
	command    *exec.Cmd
	waitErr    error
	configPath string
	started    bool
	done       chan struct{}
	closeOnce  sync.Once
}

func NewProcess(config ProcessConfig) (*Process, error) {
	config.Executable = strings.TrimSpace(config.Executable)
	config.RuntimeDir = strings.TrimSpace(config.RuntimeDir)
	if !filepath.IsAbs(config.Executable) {
		return nil, errors.New("MediaMTX executable must be an absolute path")
	}
	if !filepath.IsAbs(config.RuntimeDir) {
		return nil, errors.New("MediaMTX runtime directory must be an absolute path")
	}
	if len(bytes.TrimSpace(config.Configuration)) == 0 {
		return nil, errors.New("MediaMTX configuration is required")
	}
	if config.Readiness == nil {
		return nil, errors.New("MediaMTX readiness probe is required")
	}
	if config.StartupTimeout <= 0 {
		config.StartupTimeout = defaultStartupTimeout
	}
	if config.StopTimeout <= 0 {
		config.StopTimeout = defaultStopTimeout
	}
	if config.Stdout == nil {
		config.Stdout = os.Stdout
	}
	if config.Stderr == nil {
		config.Stderr = os.Stderr
	}
	return &Process{config: config, done: make(chan struct{})}, nil
}

func (process *Process) Start(ctx context.Context) error {
	process.mu.Lock()
	if process.started {
		process.mu.Unlock()
		return errors.New("MediaMTX process was already started")
	}
	process.started = true
	process.mu.Unlock()

	versionContext, cancelVersion := context.WithTimeout(ctx, 3*time.Second)
	versionCommand := exec.CommandContext(versionContext, process.config.Executable, "--version")
	versionOutput, err := versionCommand.Output()
	cancelVersion()
	if err != nil {
		return fmt.Errorf("verify MediaMTX version: %w", err)
	}
	if actual := strings.TrimSpace(string(versionOutput)); actual != Version {
		return fmt.Errorf("MediaMTX version is %q, want pinned %q", actual, Version)
	}

	if err := os.MkdirAll(process.config.RuntimeDir, 0o750); err != nil {
		return fmt.Errorf("create MediaMTX runtime directory: %w", err)
	}
	configPath, err := writeConfiguration(process.config.RuntimeDir, process.config.Configuration)
	if err != nil {
		return err
	}
	process.mu.Lock()
	process.configPath = configPath
	process.mu.Unlock()

	command := exec.Command(process.config.Executable, configPath)
	command.Dir = process.config.RuntimeDir
	command.Stdout = process.config.Stdout
	command.Stderr = process.config.Stderr
	if err := command.Start(); err != nil {
		_ = os.Remove(configPath)
		return fmt.Errorf("start MediaMTX: %w", err)
	}
	process.mu.Lock()
	process.command = command
	process.mu.Unlock()
	go func() {
		err := command.Wait()
		process.mu.Lock()
		process.waitErr = err
		process.mu.Unlock()
		close(process.done)
	}()

	startupContext, cancelStartup := context.WithTimeout(ctx, process.config.StartupTimeout)
	defer cancelStartup()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	var lastProbeErr error
	for {
		if err := process.config.Readiness(startupContext); err == nil {
			return nil
		} else {
			lastProbeErr = err
		}
		select {
		case <-process.done:
			return fmt.Errorf("MediaMTX exited before readiness: %w", process.exitError())
		case <-startupContext.Done():
			_ = process.Close()
			if lastProbeErr != nil {
				return fmt.Errorf("wait for MediaMTX readiness: %w: last probe: %v", startupContext.Err(), lastProbeErr)
			}
			return fmt.Errorf("wait for MediaMTX readiness: %w", startupContext.Err())
		case <-ticker.C:
		}
	}
}

func (process *Process) Done() <-chan struct{} {
	return process.done
}

func (process *Process) Err() error {
	select {
	case <-process.done:
		return process.exitError()
	default:
		return nil
	}
}

func (process *Process) exitError() error {
	process.mu.Lock()
	defer process.mu.Unlock()
	if process.waitErr == nil {
		return errors.New("MediaMTX exited")
	}
	return process.waitErr
}

func (process *Process) Close() error {
	if process == nil {
		return nil
	}
	var closeErr error
	process.closeOnce.Do(func() {
		process.mu.Lock()
		command := process.command
		configPath := process.configPath
		started := process.started
		process.mu.Unlock()
		if !started || command == nil || command.Process == nil {
			if configPath != "" {
				_ = os.Remove(configPath)
			}
			return
		}
		select {
		case <-process.done:
		default:
			if err := command.Process.Signal(os.Interrupt); err != nil && !errors.Is(err, os.ErrProcessDone) {
				closeErr = fmt.Errorf("interrupt MediaMTX: %w", err)
			}
		}
		timer := time.NewTimer(process.config.StopTimeout)
		defer timer.Stop()
		select {
		case <-process.done:
		case <-timer.C:
			if err := command.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) && closeErr == nil {
				closeErr = fmt.Errorf("kill MediaMTX after stop timeout: %w", err)
			}
			<-process.done
		}
		if err := os.Remove(configPath); err != nil && !errors.Is(err, os.ErrNotExist) && closeErr == nil {
			closeErr = fmt.Errorf("remove MediaMTX runtime configuration: %w", err)
		}
	})
	return closeErr
}

func writeConfiguration(runtimeDir string, content []byte) (string, error) {
	temporary, err := os.CreateTemp(runtimeDir, ".mediamtx-*.json.tmp")
	if err != nil {
		return "", fmt.Errorf("create MediaMTX configuration: %w", err)
	}
	temporaryPath := temporary.Name()
	cleanup := func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}
	if err := temporary.Chmod(0o600); err != nil {
		cleanup()
		return "", fmt.Errorf("protect MediaMTX configuration: %w", err)
	}
	if _, err := temporary.Write(content); err != nil {
		cleanup()
		return "", fmt.Errorf("write MediaMTX configuration: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		cleanup()
		return "", fmt.Errorf("sync MediaMTX configuration: %w", err)
	}
	if err := temporary.Close(); err != nil {
		cleanup()
		return "", fmt.Errorf("close MediaMTX configuration: %w", err)
	}
	finalPath := filepath.Join(runtimeDir, "mediamtx.json")
	if err := os.Rename(temporaryPath, finalPath); err != nil {
		cleanup()
		return "", fmt.Errorf("publish MediaMTX configuration: %w", err)
	}
	return finalPath, nil
}
