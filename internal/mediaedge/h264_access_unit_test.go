package mediaedge

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/pion/rtp"
)

func TestH264AssemblerBuildsSTAPAKeyframe(t *testing.T) {
	sps := []byte{0x67, 0x42, 0xe0, 0x1f}
	pps := []byte{0x68, 0xce, 0x06}
	idr := []byte{0x65, 0x88, 0x84}
	packet := &rtp.Packet{
		Header: rtp.Header{
			Version: 2, PayloadType: 96, SequenceNumber: 10,
			Timestamp: 90_000, Marker: true,
		},
		Payload: stapAPayload(sps, pps, idr),
	}

	var assembler h264AccessUnitAssembler
	result := assembler.Push(packet, time.Unix(100, 0))
	if result.Discontinuity || result.AccessUnit == nil {
		t.Fatalf("valid STAP-A result = %+v", result)
	}
	accessUnit := result.AccessUnit
	if !accessUnit.Keyframe || !accessUnit.HasSPS || !accessUnit.HasPPS {
		t.Fatalf("STAP-A keyframe metadata = %+v", accessUnit)
	}
	if len(accessUnit.NALUs) != 3 ||
		!bytes.Equal(accessUnit.NALUs[0], sps) ||
		!bytes.Equal(accessUnit.NALUs[1], pps) ||
		!bytes.Equal(accessUnit.NALUs[2], idr) {
		t.Fatalf("STAP-A NAL units = %x", accessUnit.NALUs)
	}
}

func TestH264AssemblerReassemblesFUAAndRejectsPacketGap(t *testing.T) {
	var assembler h264AccessUnitAssembler
	first := &rtp.Packet{
		Header: rtp.Header{
			Version: 2, PayloadType: 96, SequenceNumber: 20, Timestamp: 12_000,
		},
		// FU indicator preserves F/NRI, FU header supplies IDR type and start.
		Payload: []byte{0x7c, 0x85, 0xaa, 0xbb},
	}
	second := &rtp.Packet{
		Header: rtp.Header{
			Version: 2, PayloadType: 96, SequenceNumber: 21, Timestamp: 12_000, Marker: true,
		},
		Payload: []byte{0x7c, 0x45, 0xcc},
	}
	if result := assembler.Push(first, time.Now()); result.AccessUnit != nil || result.Discontinuity {
		t.Fatalf("FU-A start unexpectedly completed: %+v", result)
	}
	result := assembler.Push(second, time.Now())
	if result.Discontinuity || result.AccessUnit == nil || !result.AccessUnit.Keyframe {
		t.Fatalf("FU-A completion = %+v", result)
	}
	if got, want := result.AccessUnit.NALUs[0], []byte{0x65, 0xaa, 0xbb, 0xcc}; !bytes.Equal(got, want) {
		t.Fatalf("reassembled FU-A = %x, want %x", got, want)
	}

	gapped := &rtp.Packet{
		Header: rtp.Header{
			Version: 2, PayloadType: 96, SequenceNumber: 24, Timestamp: 15_000, Marker: true,
		},
		Payload: []byte{0x41, 0x01},
	}
	result = assembler.Push(gapped, time.Now())
	if !result.Discontinuity || result.LostPackets != 2 {
		t.Fatalf("packet gap was not reported: %+v", result)
	}
}

func TestAnnexBPrependsCachedParameterSets(t *testing.T) {
	accessUnit := &h264AccessUnit{
		NALUs:    [][]byte{{0x65, 0x01}},
		Keyframe: true,
	}
	got := accessUnitAnnexB(
		accessUnit,
		[]byte{0x67, 0x02},
		[]byte{0x68, 0x03},
	)
	want := []byte{
		0, 0, 0, 1, 0x67, 0x02,
		0, 0, 0, 1, 0x68, 0x03,
		0, 0, 0, 1, 0x65, 0x01,
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Annex-B access unit = %x, want %x", got, want)
	}
}

func stapAPayload(nalus ...[]byte) []byte {
	payload := []byte{0x78}
	for _, nalu := range nalus {
		length := make([]byte, 2)
		binary.BigEndian.PutUint16(length, uint16(len(nalu)))
		payload = append(payload, length...)
		payload = append(payload, nalu...)
	}
	return payload
}
