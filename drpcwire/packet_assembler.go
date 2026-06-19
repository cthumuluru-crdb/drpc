package drpcwire

import "storj.io/drpc"

// BufferSource provides reusable byte buffers. *drpcstream.BufferPool satisfies
// it structurally; the interface lives here to avoid an import cycle (drpcwire
// must not import drpcstream).
type BufferSource interface {
	Get() *[]byte
	Put(b *[]byte)
}

// PacketAssembler assembles frames into complete packets, enforcing wire
// protocol invariants:
//   - All frames must belong to the same stream ID (set explicitly via
//     SetStreamID, or inferred from the first frame).
//   - Message IDs must be monotonically increasing.
//   - Frame kind must not change within a single packet (multi-frame).
//
// It is not safe for concurrent use.
//
// When a BufferSource is set via SetPool, each completed packet's Data is backed
// by a buffer drawn from the pool, and AppendFrame returns that buffer so the
// caller can take ownership (e.g. hand it to the receive queue without copying)
// and return it to the pool when done. Without a pool, the assembler reuses a
// single backing array across packets and returns a nil owned buffer.
type PacketAssembler struct {
	pk                Packet
	assembling        bool
	streamInitialized bool
	pool              BufferSource
	held              *[]byte // pooled buffer backing pk.Data when pool != nil
}

// NewPacketAssembler returns a new PacketAssembler ready to assemble frames.
func NewPacketAssembler() PacketAssembler {
	return PacketAssembler{
		pk: Packet{
			ID: ID{Stream: 0, Message: 1},
		},
	}
}

// SetStreamID sets the expected stream ID. Frames for a different stream will
// be rejected. If not called, the stream ID is inferred from the first frame.
func (pa *PacketAssembler) SetStreamID(streamID uint64) {
	pa.pk.ID.Stream = streamID
	pa.streamInitialized = true
}

// SetPool enables pool-backed assembly. See the PacketAssembler doc comment.
func (pa *PacketAssembler) SetPool(pool BufferSource) {
	pa.pool = pool
}

// Reset clears all assembly state, preparing the assembler for a new stream. Any
// in-progress pooled buffer is returned to the pool.
func (pa *PacketAssembler) Reset() {
	if pa.pool != nil && pa.held != nil {
		pa.pool.Put(pa.held)
	}
	pa.held = nil
	pa.pk = Packet{
		ID: ID{Stream: 0, Message: 1},
	}
	pa.assembling = false
	pa.streamInitialized = false
}

// AppendFrame adds a frame to the in-progress packet. It returns the completed
// packet and true when a frame with Done=true is received. It returns false
// when more frames are needed to complete the packet.
//
// When the assembler is pool-backed (see SetPool), a completed packet's owned
// buffer is returned in owned; the caller takes ownership and must return it to
// the pool when finished with packet.Data. owned is nil when not pool-backed or
// when the packet is not yet complete.
func (pa *PacketAssembler) AppendFrame(
	fr Frame,
) (packet Packet, owned *[]byte, packetReady bool, err error) {
	// Enforce stream ID consistency: infer from first frame or reject mismatches.
	if !pa.streamInitialized {
		pa.pk.ID.Stream = fr.ID.Stream
		pa.streamInitialized = true
	} else if fr.ID.Stream != pa.pk.ID.Stream {
		return Packet{}, nil, false, drpc.ProtocolError.New(
			"frame stream mismatch: got stream %d, expected %d", fr.ID.Stream, pa.pk.ID.Stream)
	}

	if fr.ID.Message < pa.pk.ID.Message {
		return Packet{}, nil, false, drpc.ProtocolError.New(
			"message id monotonicity violation: got %v, expected >= %v", fr.ID.Message, pa.pk.ID.Message)
	} else if fr.ID.Message > pa.pk.ID.Message || !pa.assembling {
		// New message: start a fresh buffer and start assembling.
		if pa.pool != nil {
			// Defensive: release any buffer left from an incomplete prior message.
			if pa.held != nil {
				pa.pool.Put(pa.held)
			}
			pa.held = pa.pool.Get()
			pa.pk.Data = (*pa.held)[:0]
		} else {
			pa.pk.Data = pa.pk.Data[:0]
		}
		pa.assembling = true
		pa.pk.ID.Message = fr.ID.Message
	} else if fr.Kind != pa.pk.Kind {
		return Packet{}, nil, false, drpc.ProtocolError.New(
			"frame kind changed mid-packet: got %v, expected %v", fr.Kind, pa.pk.Kind)
	}

	pa.pk.Data = append(pa.pk.Data, fr.Data...)
	pa.pk.Kind = fr.Kind
	pa.pk.Control = fr.Control
	if pa.pool != nil {
		// Keep held in sync in case append grew (reallocated) the slice.
		*pa.held = pa.pk.Data
	}

	if !fr.Done {
		return Packet{}, nil, false, nil
	}

	packet = pa.pk
	owned = pa.held

	pa.assembling = false
	pa.pk.ID.Message = fr.ID.Message + 1
	if pa.pool != nil {
		// Ownership of held transfers to the caller; the next message gets a
		// fresh pooled buffer.
		pa.held = nil
		pa.pk.Data = nil
	} else {
		// Reuse the backing array: the caller must consume packet.Data before the
		// next AppendFrame call, as it will be overwritten.
		pa.pk.Data = pa.pk.Data[:0]
	}
	return packet, owned, true, nil
}
