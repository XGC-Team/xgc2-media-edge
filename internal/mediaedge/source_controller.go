package mediaedge

import (
	"context"

	xrpc "github.com/XGC-Team/xgc2-xrpc/go"
)

// sourceController is how media-edge drives one source: the camera-source
// contract of source-control-v1, spoken either over XRPC to a source process
// (cameraControl) or to an encoder pipeline that media-edge runs itself
// (pipelineControl). Operations are named as in the contract: describe, status,
// config, configure, start, stop, request-keyframe and capture.
type sourceController interface {
	call(ctx context.Context, request sourceControlRequest) (sourceControlResponse, []byte, []byte, error)
	// boundInstance is the incarnation of the source the controller is bound to.
	boundInstance() string
	// boundReference is the service reference of the source process. A
	// pipeline source has none.
	boundReference() (xrpc.ServiceRef, bool)
	close()
}

var (
	_ sourceController = (*cameraControl)(nil)
	_ sourceController = (*pipelineControl)(nil)
)
