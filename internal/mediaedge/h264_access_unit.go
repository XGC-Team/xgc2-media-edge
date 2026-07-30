package mediaedge

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/pion/rtp"
)

const (
	h264NALUTypeIDR   = 5
	h264NALUTypeSPS   = 7
	h264NALUTypePPS   = 8
	h264NALUTypeSTAPA = 24
	h264NALUTypeFUA   = 28

	// A 64 MiB encoded access unit is already far beyond the configured camera
	// profiles. Bound malformed streams that never emit an RTP marker.
	maximumH264AccessUnitBytes = 64 << 20
)

type h264AccessUnit struct {
	Timestamp     uint32
	StartSequence uint16
	EndSequence   uint16
	ReceivedAt    time.Time
	NALUs         [][]byte
	Keyframe      bool
	HasSPS        bool
	HasPPS        bool
}

type h264PacketResult struct {
	AccessUnit    *h264AccessUnit
	Discontinuity bool
	LostPackets   uint64
}

// h264AccessUnitAssembler depacketizes the packetization-mode=1 subset used by
// the source contract: single NAL units, STAP-A, and FU-A. It deliberately
// rejects malformed or incomplete access units instead of handing damaged
// bytes to the container muxer.
type h264AccessUnitAssembler struct {
	sequenceInitialized bool
	expectedSequence    uint16

	haveAccessUnit bool
	timestamp      uint32
	startSequence  uint16
	endSequence    uint16
	receivedAt     time.Time
	nalus          [][]byte
	fragment       []byte
	payloadBytes   int
	damaged        bool
}

func (assembler *h264AccessUnitAssembler) resetAccessUnit() {
	assembler.haveAccessUnit = false
	assembler.timestamp = 0
	assembler.startSequence = 0
	assembler.endSequence = 0
	assembler.receivedAt = time.Time{}
	assembler.nalus = nil
	assembler.fragment = nil
	assembler.payloadBytes = 0
	assembler.damaged = false
}

func (assembler *h264AccessUnitAssembler) Reset() {
	assembler.sequenceInitialized = false
	assembler.expectedSequence = 0
	assembler.resetAccessUnit()
}

func (assembler *h264AccessUnitAssembler) Push(packet *rtp.Packet, receivedAt time.Time) h264PacketResult {
	result := h264PacketResult{}
	if packet == nil || len(packet.Payload) == 0 {
		result.Discontinuity = true
		assembler.Reset()
		return result
	}

	if assembler.sequenceInitialized && packet.SequenceNumber != assembler.expectedSequence {
		result.Discontinuity = true
		result.LostPackets = estimateRTPPacketLoss(assembler.expectedSequence, packet.SequenceNumber)
		assembler.resetAccessUnit()
	}
	assembler.sequenceInitialized = true
	assembler.expectedSequence = packet.SequenceNumber + 1

	if assembler.haveAccessUnit && packet.Timestamp != assembler.timestamp {
		// A timestamp transition without the previous marker means the previous
		// access unit was truncated. Discard it and require a fresh IDR.
		result.Discontinuity = true
		assembler.resetAccessUnit()
	}
	if !assembler.haveAccessUnit {
		assembler.haveAccessUnit = true
		assembler.timestamp = packet.Timestamp
		assembler.startSequence = packet.SequenceNumber
		assembler.receivedAt = receivedAt
	}
	assembler.endSequence = packet.SequenceNumber

	assembler.payloadBytes += len(packet.Payload)
	if assembler.payloadBytes > maximumH264AccessUnitBytes {
		assembler.damaged = true
		result.Discontinuity = true
	} else if err := assembler.appendPayload(packet.Payload); err != nil {
		assembler.damaged = true
		result.Discontinuity = true
	}
	if !packet.Marker {
		return result
	}

	if assembler.damaged || len(assembler.fragment) != 0 || len(assembler.nalus) == 0 {
		result.Discontinuity = true
		assembler.resetAccessUnit()
		return result
	}
	accessUnit := &h264AccessUnit{
		Timestamp: assembler.timestamp, StartSequence: assembler.startSequence,
		EndSequence: assembler.endSequence, ReceivedAt: assembler.receivedAt,
		NALUs: assembler.nalus,
	}
	for _, nalu := range accessUnit.NALUs {
		switch nalu[0] & 0x1f {
		case h264NALUTypeIDR:
			accessUnit.Keyframe = true
		case h264NALUTypeSPS:
			accessUnit.HasSPS = true
		case h264NALUTypePPS:
			accessUnit.HasPPS = true
		}
	}
	result.AccessUnit = accessUnit
	assembler.resetAccessUnit()
	return result
}

func (assembler *h264AccessUnitAssembler) appendPayload(payload []byte) error {
	if len(payload) == 0 {
		return errors.New("empty H264 RTP payload")
	}
	naluType := payload[0] & 0x1f
	switch {
	case naluType >= 1 && naluType <= 23:
		if len(assembler.fragment) != 0 {
			return errors.New("single NAL unit interrupted an FU-A fragment")
		}
		assembler.appendNALU(payload)
		return nil
	case naluType == h264NALUTypeSTAPA:
		if len(assembler.fragment) != 0 {
			return errors.New("STAP-A interrupted an FU-A fragment")
		}
		return assembler.appendSTAPA(payload[1:])
	case naluType == h264NALUTypeFUA:
		return assembler.appendFUA(payload)
	default:
		return fmt.Errorf("unsupported H264 RTP NAL unit type %d", naluType)
	}
}

func (assembler *h264AccessUnitAssembler) appendSTAPA(payload []byte) error {
	if len(payload) < 2 {
		return errors.New("truncated STAP-A payload")
	}
	for len(payload) > 0 {
		if len(payload) < 2 {
			return errors.New("truncated STAP-A NAL length")
		}
		length := int(binary.BigEndian.Uint16(payload[:2]))
		payload = payload[2:]
		if length == 0 || length > len(payload) {
			return errors.New("invalid STAP-A NAL length")
		}
		assembler.appendNALU(payload[:length])
		payload = payload[length:]
	}
	return nil
}

func (assembler *h264AccessUnitAssembler) appendFUA(payload []byte) error {
	if len(payload) < 3 {
		return errors.New("truncated FU-A payload")
	}
	indicator := payload[0]
	header := payload[1]
	start := header&0x80 != 0
	end := header&0x40 != 0
	if header&0x20 != 0 {
		return errors.New("reserved FU-A bit is set")
	}
	fragmentType := header & 0x1f
	if fragmentType < 1 || fragmentType > 23 {
		return errors.New("FU-A carries an invalid fragmented NAL type")
	}
	if start && end {
		return errors.New("FU-A start and end bits are both set")
	}
	if start {
		if len(assembler.fragment) != 0 {
			return errors.New("nested FU-A start")
		}
		reconstructedHeader := indicator&0xe0 | fragmentType
		assembler.fragment = append([]byte{reconstructedHeader}, payload[2:]...)
		if end {
			assembler.appendNALU(assembler.fragment)
			assembler.fragment = nil
		}
		return nil
	}
	if len(assembler.fragment) == 0 {
		return errors.New("FU-A continuation has no start")
	}
	assembler.fragment = append(assembler.fragment, payload[2:]...)
	if end {
		assembler.appendNALU(assembler.fragment)
		assembler.fragment = nil
	}
	return nil
}

func (assembler *h264AccessUnitAssembler) appendNALU(nalu []byte) {
	assembler.nalus = append(assembler.nalus, append([]byte(nil), nalu...))
}

func estimateRTPPacketLoss(expected uint16, actual uint16) uint64 {
	delta := uint16(actual - expected)
	if delta == 0 {
		return 0
	}
	// A large modular delta is more likely an out-of-order or duplicate packet
	// than the loss of almost the complete uint16 sequence space.
	if delta >= 1<<15 {
		return 1
	}
	return uint64(delta)
}

func accessUnitAnnexB(accessUnit *h264AccessUnit, cachedSPS []byte, cachedPPS []byte) []byte {
	if accessUnit == nil {
		return nil
	}
	size := 0
	if !accessUnit.HasSPS {
		size += 4 + len(cachedSPS)
	}
	if !accessUnit.HasPPS {
		size += 4 + len(cachedPPS)
	}
	for _, nalu := range accessUnit.NALUs {
		size += 4 + len(nalu)
	}
	output := make([]byte, 0, size)
	appendNALU := func(nalu []byte) {
		if len(nalu) == 0 {
			return
		}
		output = append(output, 0, 0, 0, 1)
		output = append(output, nalu...)
	}
	if !accessUnit.HasSPS {
		appendNALU(cachedSPS)
	}
	if !accessUnit.HasPPS {
		appendNALU(cachedPPS)
	}
	for _, nalu := range accessUnit.NALUs {
		appendNALU(nalu)
	}
	return output
}
