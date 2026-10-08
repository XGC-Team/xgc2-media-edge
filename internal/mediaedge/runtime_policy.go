package mediaedge

import (
	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
	"github.com/XGC-Team/xgc2-xrpc/go/httpx"
	"strconv"
	"time"
)

// ResolveRuntimePolicy is called once by the composition root with its explicit
// environment snapshot. The product does not silently select settings whose
// owner is absent: diagnostics, dynamic reference registry and gRPC are rejected.
func ResolveRuntimePolicy(environment []string, config Config) (*xrpc.Policy, error) {
	normalized, err := config.normalized()
	if err != nil {
		return nil, err
	}
	return xrpc.ResolvePolicy(xrpc.PolicyOptions{
		Environment:   append([]string(nil), environment...),
		Capabilities:  []string{"host", "http", "rpc", "transport", "client_pool"},
		DefaultSource: "media-edge safety contract",
		Defaults: map[string]string{
			"HOST_MAX_CONNECTIONS": "32", "HOST_MAX_IN_FLIGHT": strconv.Itoa(normalized.MaxOperations),
			"MAX_HEADER_BYTES": "16384", "MAX_REQUEST_BYTES": "307200",
			"MAX_RESPONSE_BYTES": strconv.FormatInt(normalized.MaxCaptureBytes+maximumControlHeaderBytes+4*maximumCameraPartHeaderBytes, 10),
			"CALL_TIMEOUT_MS":    "15000", "HEADER_TIMEOUT_MS": "3000", "IDLE_TIMEOUT_MS": "30000", "SHUTDOWN_TIMEOUT_MS": "5000", "CLIENT_MAX_CONNECTIONS": "2",
		},
		Ceilings: map[string]int64{
			"HOST_MAX_CONNECTIONS": 128, "HOST_MAX_IN_FLIGHT": int64(normalized.MaxOperations), "MAX_HEADER_BYTES": 16384, "MAX_REQUEST_BYTES": 307200,
			"MAX_RESPONSE_BYTES": normalized.MaxCaptureBytes + maximumControlHeaderBytes + 4*maximumCameraPartHeaderBytes,
			"CALL_TIMEOUT_MS":    30000, "HEADER_TIMEOUT_MS": 10000, "IDLE_TIMEOUT_MS": 60000, "SHUTDOWN_TIMEOUT_MS": 10000, "CLIENT_MAX_CONNECTIONS": 2,
		},
	})
}

func (config Config) hostOptions(private bool) (httpx.HostOptions, error) {
	options := httpx.HostOptions{MaxConnections: 32, MaxInFlight: config.MaxOperations, MaxBodyBytes: 300 << 10, MaxResponseBytes: config.MaxCaptureBytes + maximumControlHeaderBytes + 4*maximumCameraPartHeaderBytes, MaxHeaderBytes: 16 << 10, HeaderTimeout: 3 * time.Second, MaxCallTime: 15 * time.Second, WriteTimeout: 20 * time.Second, IdleTimeout: 30 * time.Second, ShutdownTimeout: 5 * time.Second}
	if config.RuntimePolicy != nil {
		var err error
		options, err = (httpx.HostOptions{WriteTimeout: 20 * time.Second}).WithPolicy(config.RuntimePolicy)
		if err != nil {
			return options, err
		}
	}
	if private {
		options.WriteTimeout = options.MaxCallTime
	}
	return options, nil
}

func (config Config) effectivePolicy() xrpc.EffectivePolicy {
	if config.RuntimePolicy == nil {
		return xrpc.EffectivePolicy{}
	}
	return config.RuntimePolicy.Effective()
}

func (server *MediaMTXServer) shutdownTimeout() time.Duration {
	if server.config.RuntimePolicy != nil {
		if value, err := server.config.RuntimePolicy.Integer("SHUTDOWN_TIMEOUT_MS"); err == nil {
			return time.Duration(value) * time.Millisecond
		}
	}
	return 5 * time.Second
}
