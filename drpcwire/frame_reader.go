// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package drpcwire

import (
	"io"

	"storj.io/drpc"
)

// frameReaderOptions controls configuration settings for a reader.
type frameReaderOptions struct {
	// MaximumBufferSize controls the maximum size of buffered
	// packet data.
	MaximumBufferSize int
}

type frameReader struct {
	opts frameReaderOptions
	r    io.Reader
	curr []byte
	buf  []byte
	rerr error
}

// A frame adds at most this many bytes of overhead to some data by prefixing
// the data with:
//
//	1: control byte
//	9: maximum varint stream id
//	9: maximum varint message id
//	9: maximum varint data length
const maxFrameOverhead = 1 + 9 + 9 + 9

// NewReader constructs a Reader to read Packets from the io.Reader.
func newFrameReader(r io.Reader) *frameReader {
	return newFrameReaderWithOptions(r, frameReaderOptions{})
}

// NewReaderWithOptions constructs a Reader to read Packets from
// the io.Reader. It uses the provided options to manage buffering.
func newFrameReaderWithOptions(r io.Reader, opts frameReaderOptions) *frameReader {
	if opts.MaximumBufferSize == 0 {
		opts.MaximumBufferSize = 4 << 20 // Default to 4MiB.
	}

	return &frameReader{
		opts: opts,
		r:    r,
		// Err on the side of a smaller buffer since ReadPacket will lazily
		// grow this buffer.
		curr: make([]byte, 0, 4096),
	}
}

// read calls Read on the underlying reader and ensures the the return
// value is (>0, nil) or (0, err).
func (r *frameReader) read(p []byte) (n int, err error) {
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

// ReadFrameUsing reads the next complete Frame from the underlying reader,
// buffering partial data in r.buf until a full frame is available.
func (r *frameReader) ReadFrameUsing() (fr Frame, err error) {
	for {
		var ok bool
		r.curr, fr, ok, err = ParseFrame(r.curr)
		switch {
		case err != nil:
			return Frame{}, drpc.ProtocolError.Wrap(err)

		case !ok:
			// r.curr doesn't have enough data for a full frame, so prepend
			// it to the read buffer if it is in the appropriate state.
			if len(r.buf) == 0 {
				r.buf = append(r.buf[:0], r.curr...)
			}

			if cap(r.buf)-len(r.buf) < 4096 {
				nbuf := make([]byte, len(r.buf), 2*cap(r.buf)+4096)
				copy(nbuf, r.buf)
				r.buf = nbuf
			}

			n, err := r.read(r.buf[len(r.buf):cap(r.buf)])
			if err != nil {
				return Frame{}, err
			}

			ncap := uint(len(r.buf) + n)
			if ncap > uint(cap(r.buf)) {
				return Frame{}, drpc.ProtocolError.New("data overflow")
			}
			r.buf = r.buf[:ncap]

			if len(r.buf)-maxFrameOverhead > r.opts.MaximumBufferSize {
				return Frame{}, drpc.ProtocolError.New("data overflow")
			}

			r.curr = r.buf
			continue
		}

		// since we got a frame, signal that we need to restore buf with
		// whatever remains in r.curr the next time we don't have a frame.
		if len(r.buf) > 0 {
			r.buf = r.buf[:0]
		}

		return fr, nil
	}
}
