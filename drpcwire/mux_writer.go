// Copyright (C) 2026 Cockroach Labs.
// See LICENSE for copying information.

package drpcwire

import (
	"bufio"
	"io"
	"runtime"
	"sync"
)

// frameNode is a singly-linked list node holding one queued frame.
type frameNode struct {
	fr       Frame
	ownedBuf *[]byte // non-nil when fr.Data is a pool allocation; returned after serialization
	next     *frameNode
}

// frameDataPool recycles the owned []byte copies of Frame.Data to reduce GC
// pressure. Each pool entry is a *[]byte so the backing array can be resized
// without losing the pool pointer.
var frameDataPool = sync.Pool{New: func() any { return new([]byte) }}

const (
	// defaultBufioSize is 4× the 64 KB default SplitSize, giving room for
	// several max-sized frames before bufio auto-flushes.
	defaultBufioSize = 256 * 1024

	// minBatchSize mirrors gRPC loopyWriter: if total bytes written this drain
	// cycle are below this threshold, Gosched once before returning to the
	// blocking wait, giving producers a chance to enqueue more frames.
	minBatchSize = 1000
)

// MuxWriter serializes frames from multiple concurrent producers onto a single
// io.Writer. Producers enqueue Frame objects in O(1) under a mutex; all
// serialization happens in a dedicated writer goroutine.
//
// Flush semantics: the writer goroutine writes frames into a bufio.Writer.
// When any frame in a drain batch has Done=true, the bufio buffer is flushed
// explicitly to the underlying writer. Otherwise, flushing is left to bufio's
// auto-flush (when its internal buffer fills).
type MuxWriter struct {
	w        io.Writer
	bw       *bufio.Writer
	head     *frameNode // sentinel; head.next is the first real node
	tail     *frameNode // points to last node (== head when empty)
	mu       sync.Mutex
	cond     *sync.Cond
	closed   bool
	closeErr error
	onError  func(error)
	done     chan struct{}
}

func NewMuxWriter(w io.Writer, onError func(error)) *MuxWriter {
	sentinel := &frameNode{}
	mw := &MuxWriter{
		w:       w,
		bw:      bufio.NewWriterSize(w, defaultBufioSize),
		head:    sentinel,
		tail:    sentinel,
		onError: onError,
		done:    make(chan struct{}),
	}
	mw.cond = sync.NewCond(&mw.mu)
	go mw.run()
	return mw
}

func (mw *MuxWriter) run() {
	defer close(mw.done)

	for {
		// Phase 1: block until the queue is non-empty or closed.
		mw.mu.Lock()
		for mw.head == mw.tail && !mw.closed {
			mw.cond.Wait()
		}
		if mw.closed {
			mw.mu.Unlock()
			return
		}

		// Phase 2: drain loop. Mirrors gRPC loopyWriter's hasdata pattern:
		// drain all available frames into bufio, flush if any had Done=true,
		// then Gosched once if the batch was small before returning to Phase 1.
		gosched := true
		batchSize := 0
	hasdata:
		for {
			// Atomically detach the entire current queue chain.
			// Lock is held on entry to this point every iteration.
			first := mw.head.next
			if first != nil {
				mw.head.next = nil
				mw.tail = mw.head
			}
			mw.mu.Unlock()

			if first == nil {
				// Queue empty: spin once if batch was small.
				if gosched && batchSize < minBatchSize {
					gosched = false
					runtime.Gosched()
					mw.mu.Lock()
					if mw.closed {
						mw.mu.Unlock()
						return
					}
					continue hasdata
				}
				break hasdata
			}

			// Serialize the detached chain into bufio (no lock needed).
			needFlush := false
			for n := first; n != nil; n = n.next {
				written, err := WriteFrameTo(mw.bw, n.fr)
				batchSize += written
				needFlush = needFlush || n.fr.Done
				if n.ownedBuf != nil {
					frameDataPool.Put(n.ownedBuf)
					n.ownedBuf = nil
				}
				if err != nil {
					mw.mu.Lock()
					if !mw.closed {
						mw.closed = true
						mw.closeErr = err
					}
					mw.mu.Unlock()
					if mw.onError != nil {
						mw.onError(err)
					}
					return
				}
			}

			if needFlush {
				if err := mw.bw.Flush(); err != nil {
					mw.mu.Lock()
					if !mw.closed {
						mw.closed = true
						mw.closeErr = err
					}
					mw.mu.Unlock()
					if mw.onError != nil {
						mw.onError(err)
					}
					return
				}
			}

			mw.mu.Lock()
			if mw.closed {
				mw.mu.Unlock()
				return
			}
			continue hasdata
		}
	}
}

// WriteFrame enqueues fr for delivery to the underlying writer. The caller may
// reuse or mutate fr.Data immediately after WriteFrame returns; the data is
// copied into a pool-managed buffer before enqueue.
func (mw *MuxWriter) WriteFrame(fr Frame) error {
	// Copy Data outside the lock so serialization is off the critical path and
	// the caller can safely reuse its buffer immediately after this returns.
	var ownedBuf *[]byte
	if len(fr.Data) > 0 {
		p := frameDataPool.Get().(*[]byte)
		if cap(*p) < len(fr.Data) {
			*p = make([]byte, len(fr.Data))
		}
		*p = (*p)[:len(fr.Data)]
		copy(*p, fr.Data)
		fr.Data = *p
		ownedBuf = p
	}

	node := &frameNode{fr: fr, ownedBuf: ownedBuf}

	mw.mu.Lock()
	if mw.closed {
		err := mw.closeErr
		mw.mu.Unlock()
		if ownedBuf != nil {
			frameDataPool.Put(ownedBuf)
		}
		return err
	}
	mw.tail.next = node
	mw.tail = node
	mw.mu.Unlock()
	mw.cond.Signal() // signal after unlock: woken goroutine acquires lock without re-blocking
	return nil
}

func (mw *MuxWriter) Stop(err error) {
	mw.mu.Lock()
	if !mw.closed {
		mw.closed = true
		mw.closeErr = err
		mw.cond.Broadcast()
	}
	mw.mu.Unlock()
}

func (mw *MuxWriter) Done() <-chan struct{} {
	return mw.done
}
