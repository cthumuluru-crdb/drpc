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
	r    io.Reader
	fr   *frameReader
	id   ID
	rerr error
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

// read calls Read on the underlying reader and ensures the the return
// value is (>0, nil) or (0, err).
func (r *Reader) read(p []byte) (n int, err error) {
	for i := 0; i < 100; i++ {
		if r.rerr != nil {
			r.rerr, err = nil, r.rerr
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return 0, err
			}
			return 0, drpc.ConnectionError.Wrap(err)
		}
		n, r.rerr = r.r.Read(p)
		if n > 0 {
			return n, nil
		}
	}
	return 0, drpc.InternalError.Wrap(io.ErrNoProgress)
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
// returned. The returned packet's Data field is constructed by appending
// to the provided buf after it has been resliced to be zero length.
func (r *Reader) ReadPacketUsing(buf []byte) (pkt Packet, err error) {
	pkt.Data = buf[:0]

	for {
		fr, err := r.fr.ReadFrame()
		if err != nil {
			return Packet{}, err
		}

		// If any frames are set to control, then the whole packet is
		// considered to be control.
		pkt.Control = pkt.Control || fr.Control

		switch {
		case fr.ID.Less(r.id):
			return Packet{}, drpc.ProtocolError.New("id monotonicity violation (fr:%v r:%v)", fr.ID, r.id)

		case r.id != fr.ID || pkt.ID == ID{}:
			r.id = fr.ID

			pkt = Packet{
				Data:    pkt.Data[:0],
				ID:      fr.ID,
				Kind:    fr.Kind,
				Control: fr.Control,
			}

		case fr.Kind != pkt.Kind:
			return Packet{}, drpc.ProtocolError.New("packet kind change (fr:%v pkt:%v)", fr.Kind, pkt.Kind)
		}

		pkt.Data = append(pkt.Data, fr.Data...)

		switch {
		case len(pkt.Data) > r.opts.MaximumBufferSize:
			return Packet{}, drpc.ProtocolError.New("data overflow (len:%v)", len(pkt.Data))

		case fr.Done:
			// increment the message id so that we do not accept any frames
			// with the same id.
			r.id.Message++
			return pkt, nil
		}
	}
}
