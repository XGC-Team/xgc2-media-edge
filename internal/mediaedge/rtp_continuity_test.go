package mediaedge

import (
	"math"
	"testing"

	"github.com/pion/rtp"
)

func TestRTPContinuityPreservesNormalGaps(t *testing.T) {
	rewriter := newRTPContinuityRewriter(20)
	packets := []*rtp.Packet{
		rtpPacket(100, 90_000),
		rtpPacket(104, 103_500),
		rtpPacket(105, 103_500),
	}

	for _, packet := range packets {
		sequence, timestamp := packet.SequenceNumber, packet.Timestamp
		rewriter.rewrite(packet)
		if packet.SequenceNumber != sequence || packet.Timestamp != timestamp {
			t.Fatalf(
				"normal RTP gap was rewritten: got seq=%d timestamp=%d, want seq=%d timestamp=%d",
				packet.SequenceNumber, packet.Timestamp, sequence, timestamp,
			)
		}
	}
}

func TestRTPContinuityPreservesUintWrap(t *testing.T) {
	rewriter := newRTPContinuityRewriter(30)
	packets := []*rtp.Packet{
		rtpPacket(math.MaxUint16-1, math.MaxUint32-2_000),
		rtpPacket(1, 999),
	}

	for _, packet := range packets {
		sequence, timestamp := packet.SequenceNumber, packet.Timestamp
		rewriter.rewrite(packet)
		if packet.SequenceNumber != sequence || packet.Timestamp != timestamp {
			t.Fatalf(
				"natural RTP wrap was rewritten: got seq=%d timestamp=%d, want seq=%d timestamp=%d",
				packet.SequenceNumber, packet.Timestamp, sequence, timestamp,
			)
		}
	}
}

func TestRTPContinuityRemapsRestartReset(t *testing.T) {
	rewriter := newRTPContinuityRewriter(20)
	beforeRestart := []*rtp.Packet{
		rtpPacket(32_000, 9_000_000),
		rtpPacket(32_002, 9_004_500),
	}
	for _, packet := range beforeRestart {
		rewriter.rewrite(packet)
	}

	firstRestartPacket := rtpPacket(4, 0)
	rewriter.rewrite(firstRestartPacket)
	if firstRestartPacket.SequenceNumber != 32_003 || firstRestartPacket.Timestamp != 9_009_000 {
		t.Fatalf(
			"restart did not continue the output timeline: got seq=%d timestamp=%d",
			firstRestartPacket.SequenceNumber, firstRestartPacket.Timestamp,
		)
	}

	nextRestartPacket := rtpPacket(7, 9_000)
	rewriter.rewrite(nextRestartPacket)
	if nextRestartPacket.SequenceNumber != 32_006 || nextRestartPacket.Timestamp != 9_018_000 {
		t.Fatalf(
			"new segment gap was not preserved: got seq=%d timestamp=%d",
			nextRestartPacket.SequenceNumber, nextRestartPacket.Timestamp,
		)
	}
}

func TestRTPContinuityRemapsExcessiveForwardJump(t *testing.T) {
	rewriter := newRTPContinuityRewriter(30)
	first := rtpPacket(10, 100_000)
	rewriter.rewrite(first)

	jumped := rtpPacket(20, 100_000+maximumContinuousRTPTimestampGap+1)
	rewriter.rewrite(jumped)
	if jumped.SequenceNumber != 11 || jumped.Timestamp != 103_000 {
		t.Fatalf(
			"excessive jump did not start a continuous segment: got seq=%d timestamp=%d",
			jumped.SequenceNumber, jumped.Timestamp,
		)
	}
}

func rtpPacket(sequence uint16, timestamp uint32) *rtp.Packet {
	return &rtp.Packet{
		Header:  rtp.Header{Version: 2, PayloadType: 96, SequenceNumber: sequence, Timestamp: timestamp},
		Payload: []byte{0x65},
	}
}
