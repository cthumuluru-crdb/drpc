// Copyright (C) 2026 Cockroach Labs.
// See LICENSE for copying information.

package drpcmanager

import (
	"sync"

	"storj.io/drpc/drpcstream"
)

// activeStreams is a thread-safe map of stream IDs to stream objects.
// It is used by the Manager to track active streams for lifecycle management.
type activeStreams struct {
	mu       sync.RWMutex
	streams  map[uint64]*drpcstream.Stream
	closed   bool
	closeErr error
}

func newActiveStreams() *activeStreams {
	return &activeStreams{
		streams: make(map[uint64]*drpcstream.Stream),
	}
}

// Add adds a stream. It returns an error if the collection is closed or if a
// stream with the same ID already exists.
func (r *activeStreams) Add(id uint64, stream *drpcstream.Stream) error {
	if stream == nil {
		return managerClosed.New("stream can't be nil")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return r.closeErr
	}
	if _, ok := r.streams[id]; ok {
		return managerClosed.New("duplicate stream id")
	}
	r.streams[id] = stream
	return nil
}

// Remove removes a stream. It is a no-op if the stream is not present or if
// the collection has been closed.
func (r *activeStreams) Remove(id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.streams != nil {
		delete(r.streams, id)
	}
}

// Get returns the stream for the given ID and whether it was found.
func (r *activeStreams) Get(id uint64) (*drpcstream.Stream, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return nil, false
	}
	s, ok := r.streams[id]
	return s, ok
}

// Close marks the collection closed (rejecting future Add calls),
// snapshots and clears the map, and then cancels each stream outside
// the mutex. Doing the Cancel work outside the lock keeps Add/Get/Remove
// callers unblocked and avoids nesting activeStreams.mu around each
// stream's own lock.
func (r *activeStreams) Close(err error) {
	r.mu.Lock()
	r.closed = true
	r.closeErr = err
	streams := r.streams
	r.streams = nil
	r.mu.Unlock()

	for _, s := range streams {
		s.Cancel(err)
	}
}

// Len returns the number of active streams.
func (r *activeStreams) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.streams)
}
