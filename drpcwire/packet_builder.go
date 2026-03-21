// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package drpcwire

import "storj.io/drpc"

// PacketBuilder assembles frames into a packet, enforcing wire protocol
// invariants. It is not safe for concurrent use.
type PacketBuilder struct {
	pkt  Packet
	done bool
}

// AppendFrame appends a frame to the in-progress packet. It enforces the
// following invariants:
//
//   - A different stream ID, a greater message ID, or no packet in progress starts a fresh packet.
//   - A message ID less than the current packet's (same stream) is a protocol error.
//   - All frames belonging to the same packet must share the same Kind.
func (b *PacketBuilder) AppendFrame(fr Frame) error {
	switch {
	// No packet in progress, different stream ID, or greater message ID: start a fresh packet.
	case b.pkt.ID == (ID{}) || b.pkt.ID.Stream != fr.ID.Stream || b.pkt.ID.Message < fr.ID.Message:
		b.pkt = Packet{
			ID:   fr.ID,
			Kind: fr.Kind,
			Data: b.pkt.Data[:0],
		}
		b.done = false

	// Same stream but message ID regressed: protocol error.
	case fr.ID.Message < b.pkt.ID.Message:
		return drpc.ProtocolError.New("message id regression (fr:%v pkt:%v)", fr.ID, b.pkt.ID)

	// Same ID but kind changed: protocol error.
	case fr.Kind != b.pkt.Kind:
		return drpc.ProtocolError.New("packet kind change (fr:%v pkt:%v)", fr.Kind, b.pkt.Kind)
	}

	b.pkt.Data = append(b.pkt.Data, fr.Data...)
	b.pkt.Control = b.pkt.Control || fr.Control

	if fr.Done {
		b.done = true
	}

	return nil
}

// Build returns the assembled packet and true when the packet is complete
// (i.e. the last frame had Done set). It resets the builder so it can be
// reused for the next packet. Returns an empty packet and false if the packet
// is not yet complete.
func (b *PacketBuilder) Build() (Packet, bool) {
	if !b.done {
		return Packet{}, false
	}
	pkt := b.pkt
	b.pkt = Packet{}
	b.done = false

	return pkt, true
}
