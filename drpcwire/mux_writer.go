// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package drpcwire

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"storj.io/drpc/drpcdebug"
	"storj.io/drpc/drpcsignal"
)

//
// MuxWriter
//

// MuxWriter buffers frames from multiple streams and writes them to an
// io.Writer via a dedicated goroutine. Streams append frames to a shared
// buffer under a short lock (no I/O). The writer goroutine is the sole
// entity that performs transport writes, eliminating head-of-line blocking
// across multiplexed streams.
type MuxWriter struct {
	w    io.Writer
	size int

	mu  sync.Mutex
	buf []byte

	notify  chan struct{}
	done    chan struct{}
	stopped drpcsignal.Signal

	onError func(error)

	flushWaiters []chan struct{}

	empty uint32
}

// NewMuxWriter returns a MuxWriter that buffers frames and writes them to w
// via a dedicated goroutine. The buffer is flushed when it reaches size bytes
// or when Flush is called. If size is 0, it defaults to 4KB. The onError
// callback is invoked if a transport write fails; it should terminate the
// connection (e.g., call manager.terminate).
func NewMuxWriter(w io.Writer, size int, onError func(error)) *MuxWriter {
	if size == 0 {
		size = 4 * 1024
	}

	mw := &MuxWriter{
		w:       w,
		size:    size,
		buf:     make([]byte, 0, size),
		notify:  make(chan struct{}, 1),
		done:    make(chan struct{}),
		onError: onError,
	}
	go mw.run()
	return mw
}

func (mw *MuxWriter) log(what string, cb func() string) {
	if drpcdebug.Enabled {
		drpcdebug.Log(func() (_, _, _ string) { return fmt.Sprintf("<mxw %p>", mw), what, cb() })
	}
}

// wake sends a non-blocking signal to the writer goroutine.
func (mw *MuxWriter) wake() {
	select {
	case mw.notify <- struct{}{}:
	default:
	}
}

// Empty returns true if there are no bytes buffered.
func (mw *MuxWriter) Empty() bool {
	return atomic.LoadUint32(&mw.empty) == 0
}

// Flush notifies the writer goroutine to write buffered data to the
// transport. It is fire-and-forget: it returns immediately without waiting
// for the write to complete.
func (mw *MuxWriter) Flush() {
	if !mw.Empty() {
		mw.wake()
	}
}

// Stop signals the writer goroutine to exit. After Stop returns, no more
// writes will be issued to the transport.
func (mw *MuxWriter) Stop(err error) {
	mw.stopped.Set(err)
	mw.wake()
}

// Done returns a channel that is closed when the writer goroutine has exited.
func (mw *MuxWriter) Done() <-chan struct{} {
	return mw.done
}

// FlushSync wakes the writer goroutine and blocks until the current buffered
// data has been written to the transport. If the buffer is empty, it returns
// immediately.
func (mw *MuxWriter) FlushSync() {
	ch := make(chan struct{})
	mw.mu.Lock()
	mw.flushWaiters = append(mw.flushWaiters, ch)
	mw.mu.Unlock()
	mw.wake()
	<-ch
}

// appendFrame appends the serialized frame to the shared buffer. If the
// buffer reaches the size threshold, the writer goroutine is notified.
// The caller must NOT hold mw.mu.
func (mw *MuxWriter) appendFrame(fr Frame) {
	mw.mu.Lock()
	if len(mw.buf) == 0 {
		atomic.StoreUint32(&mw.empty, 1)
	}
	mw.buf = AppendFrame(mw.buf, fr)
	full := len(mw.buf) >= mw.size
	mw.mu.Unlock()

	if full {
		mw.wake()
	}
}

// run is the writer goroutine. It waits for notifications, takes the
// buffered data under a short lock, and writes it to the transport outside
// the lock.
func (mw *MuxWriter) run() {
	defer close(mw.done)

	for {
		select {
		case <-mw.notify:
		case <-mw.stopped.Signal():
			return
		}

		mw.mu.Lock()
		if len(mw.buf) == 0 {
			// Nothing to write, but still notify any FlushSync waiters.
			waiters := mw.flushWaiters
			mw.flushWaiters = nil
			mw.mu.Unlock()
			for _, ch := range waiters {
				close(ch)
			}
			continue
		}

		// Swap the buffer: take ownership of the current data and give
		// the producers a fresh (or recycled) buffer. This keeps the
		// lock duration to a memcpy, never I/O.
		toWrite := mw.buf
		mw.buf = make([]byte, 0, mw.size)
		atomic.StoreUint32(&mw.empty, 0)
		waiters := mw.flushWaiters
		mw.flushWaiters = nil
		mw.mu.Unlock()

		mw.log("FLUSH", func() string { return fmt.Sprintf("write: %d", len(toWrite)) })

		if _, err := mw.w.Write(toWrite); err != nil {
			for _, ch := range waiters {
				close(ch)
			}
			mw.onError(err)
			return
		}
		for _, ch := range waiters {
			close(ch)
		}
	}
}

//
// FrameWriter
//

// FrameWriter serializes frames for a single stream and appends them to a
// MuxWriter's shared buffer. It is the per-stream write handle that replaces
// *Writer in the multiplexed write path.
type FrameWriter struct {
	mw *MuxWriter
}

// NewFrameWriter returns a FrameWriter that appends frames to the given
// MuxWriter's shared buffer.
func NewFrameWriter(mw *MuxWriter) *FrameWriter {
	return &FrameWriter{mw: mw}
}

// WriteFrame serializes the frame and appends it to the shared buffer.
// It never performs transport I/O. Returns an error if the MuxWriter
// has been stopped.
func (fw *FrameWriter) WriteFrame(fr Frame) error {
	if fw.mw.stopped.IsSet() {
		return fw.mw.stopped.Err()
	}

	fw.mw.log("WRITE", fr.String)
	fw.mw.appendFrame(fr)
	return nil
}

// WritePacket writes the packet as a single frame.
func (fw *FrameWriter) WritePacket(pkt Packet) error {
	return fw.WriteFrame(Frame{
		Data:    pkt.Data,
		ID:      pkt.ID,
		Kind:    pkt.Kind,
		Control: pkt.Control,
		Done:    true,
	})
}

// Flush notifies the writer goroutine to flush buffered data to the
// transport. Fire-and-forget: returns immediately.
func (fw *FrameWriter) Flush() {
	fw.mw.Flush()
}

// FlushSync wakes the writer goroutine and blocks until the buffered data
// has been written to the transport.
func (fw *FrameWriter) FlushSync() {
	fw.mw.FlushSync()
}

// Empty returns true if there are no bytes buffered in the MuxWriter.
func (fw *FrameWriter) Empty() bool {
	return fw.mw.Empty()
}
