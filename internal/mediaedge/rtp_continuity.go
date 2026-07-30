package mediaedge

import (
	"math"

	"github.com/pion/rtp"
)

const (
	h264RTPClockRate = 90_000

	// A short loss gap should remain visible to the receiver. A jump beyond the
	// source stall window is instead treated as a new encoder clock segment so
	// an existing WebRTC jitter buffer does not wait on the restarted timeline.
	maximumContinuousRTPTimestampGap = 2 * h264RTPClockRate
)

// rtpContinuityRewriter maps encoder clock restarts onto one continuous RTP
// timeline. It owns no packet queue: the receive loop rewrites each packet in
// place before immediately forwarding it to the shared WebRTC track.
type rtpContinuityRewriter struct {
	initialized bool

	nominalTimestampStep uint32
	timestampOffset      uint32
	sequenceOffset       uint16

	lastInputTimestamp  uint32
	lastOutputTimestamp uint32
	lastOutputSequence  uint16
}

func newRTPContinuityRewriter(fps float64) rtpContinuityRewriter {
	step := uint32(math.Round(float64(h264RTPClockRate) / fps))
	if step == 0 {
		step = 1
	}
	return rtpContinuityRewriter{nominalTimestampStep: step}
}

// rewrite returns true when an encoder restart or excessive timestamp jump was
// hidden from WebRTC by opening a new continuity segment. Recorders use that
// signal as a hard container discontinuity and wait for a fresh IDR.
func (rewriter *rtpContinuityRewriter) rewrite(packet *rtp.Packet) bool {
	inputTimestamp := packet.Timestamp
	inputSequence := packet.SequenceNumber
	restarted := false

	if !rewriter.initialized {
		rewriter.initialized = true
	} else {
		// Signed modular subtraction classifies a natural uint32 wrap as a
		// small forward delta. Ingress is a single producer over loopback, so a
		// negative timestamp delta denotes an encoder clock restart.
		timestampDelta := int64(int32(inputTimestamp - rewriter.lastInputTimestamp))
		if timestampDelta < 0 || timestampDelta > maximumContinuousRTPTimestampGap {
			restarted = true
			outputTimestamp := rewriter.lastOutputTimestamp + rewriter.nominalTimestampStep
			outputSequence := rewriter.lastOutputSequence + 1
			rewriter.timestampOffset = outputTimestamp - inputTimestamp
			rewriter.sequenceOffset = outputSequence - inputSequence
		}
	}

	packet.Timestamp = inputTimestamp + rewriter.timestampOffset
	packet.SequenceNumber = inputSequence + rewriter.sequenceOffset
	rewriter.lastInputTimestamp = inputTimestamp
	rewriter.lastOutputTimestamp = packet.Timestamp
	rewriter.lastOutputSequence = packet.SequenceNumber
	return restarted
}
