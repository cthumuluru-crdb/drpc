// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package drpcwire

import (
	"io"

	"storj.io/drpc"
)

// ReaderOptions controls configuration settings for a reader.
type ReaderOptions struct {
	// MaximumBufferSize controls the maximum size of buffered
	// packet data.
	MaximumBufferSize int
}

// Reader reconstructs packets from frames read from an io.Reader.
type Reader struct {
	opts ReaderOptions
	fr   *frameReader

	// id, nFrames, and pkt track the current packet being assembled.
	// Will eventually be replaced with a map from stream ID to in-progress
	// packet for stream multiplexing.
	id  ID
	pkt Packet
}

// NewReader constructs a Reader to read Packets from the io.Reader.
func NewReader(r io.Reader) *Reader {
	return NewReaderWithOptions(r, ReaderOptions{})
}

// NewReaderWithOptions constructs a Reader to read Packets from
// the io.Reader. It uses the provided options to manage buffering.
func NewReaderWithOptions(r io.Reader, opts ReaderOptions) *Reader {
	if opts.MaximumBufferSize == 0 {
		opts.MaximumBufferSize = 4 << 20 // Default to 4MiB.
	}

	return &Reader{
		opts: opts,
		fr:   newFrameReaderWithOptions(r, frameReaderOptions{MaximumBufferSize: opts.MaximumBufferSize}),
		id:   ID{Stream: 1, Message: 1},
	}
}

// ReadPacket reads a packet from the io.Reader. It is equivalent to
// calling ReadPacketUsing(nil).
func (r *Reader) ReadPacket() (pkt Packet, err error) {
	return r.ReadPacketUsing(nil)
}

// ReadPacketUsing reads a packet from the io.Reader. IDs read from
// frames must be monotonically increasing. When a new ID is read, the
// old data is discarded. This allows for easier asynchronous interrupts.
// If the amount of data in the Packet becomes too large, an error is
// returned.
func (r *Reader) ReadPacketUsing(buf []byte) (pkt Packet, err error) {
	for {
		fr, err := r.fr.ReadFrame()
		if err != nil {
			return Packet{}, err
		}

		switch {
		case fr.ID.Less(r.id):
			return Packet{}, drpc.ProtocolError.New("id monotonicity violation (fr:%v r:%v)", fr.ID, r.id)

		// Frame is for a new packet.
		case r.id != fr.ID || r.pkt.ID == ID{}:
			r.id = fr.ID
			r.pkt = Packet{
				ID:   fr.ID,
				Kind: fr.Kind,
				Data: []byte{},
			}

		// Frame kind for the same packet must match.
		case fr.Kind != r.pkt.Kind:
			return Packet{}, drpc.ProtocolError.New("packet kind change (fr:%v pkt:%v)", fr.Kind, r.pkt.Kind)
		}

		r.pkt.Data = append(r.pkt.Data, fr.Data...)
		r.pkt.Control = r.pkt.Control || fr.Control

		switch {
		case len(r.pkt.Data) > r.opts.MaximumBufferSize:
			return Packet{}, drpc.ProtocolError.New("data overflow (len:%v)", len(r.pkt.Data))

		case fr.Done:
			pkt = r.pkt
			r.pkt = Packet{}
			r.id.Message++
			return pkt, nil
		}
	}
}
