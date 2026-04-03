// Copyright (C) 2026 Cockroach Labs.
// See LICENSE for copying information.

package drpcmanager

import (
	"context"
	"errors"
	"io"
	"sync"

	"storj.io/drpc"
	"storj.io/drpc/drpcsignal"
	"storj.io/drpc/drpcstream"
)

// activeStreams is a thread-safe map of stream IDs to stream objects. It
// checks the provided termination signal atomically with Add to prevent
// streams from being added on a terminated manager.
type activeStreams struct {
	mu      sync.RWMutex
	streams map[uint64]*drpcstream.Stream
	// latestID tracks the highest stream ID that's active.
	//
	// NB: This will be removed once stream multiplexing implementation is
	// complete. Only used for backwards compatibility.
	latestID uint64
	term     *drpcsignal.Signal
	tport    *drpcsignal.Signal
}

func newActiveStreams(term, tport *drpcsignal.Signal) *activeStreams {
	return &activeStreams{
		streams: make(map[uint64]*drpcstream.Stream),
		term:    term,
		tport:   tport,
	}
}

// Add adds a stream. It returns an error if the manager is terminated or if
// a stream with the same ID already exists.
func (a *activeStreams) Add(id uint64, stream *drpcstream.Stream) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if err, ok := a.term.Get(); ok {
		return err
	}
	if _, ok := a.streams[id]; ok {
		return drpc.ProtocolError.New("duplicate stream id %d", id)
	}
	a.streams[id] = stream
	// NB: Only one active stream is supported, so we can just track the latest ID.
	a.latestID = id

	return nil
}

// Remove removes a stream by ID.
func (a *activeStreams) Remove(id uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	delete(a.streams, id)
}

// Get returns the stream for the given ID and whether it was found.
func (a *activeStreams) Get(id uint64) (*drpcstream.Stream, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	s, ok := a.streams[id]
	return s, ok
}

// Latest returns the stream with the highest ID, or nil if empty.
func (a *activeStreams) Latest() *drpcstream.Stream {
	a.mu.RLock()
	defer a.mu.RUnlock()

	return a.streams[a.latestID]
}

// Len returns the number of active streams.
func (a *activeStreams) Len() int {
	a.mu.RLock()
	defer a.mu.RUnlock()

	return len(a.streams)
}

// Close cancels all active streams with the provided error and clears the map.
// After Close, Add will fail (term signal is already set by the caller), and
// Remove is a safe no-op.
func (a *activeStreams) Close(err error) {
	if !a.tport.IsSet() {
		panic("activeStreams.Close called before transport was closed")
	}

	var streams map[uint64]*drpcstream.Stream
	func() {
		a.mu.Lock()
		defer a.mu.Unlock()

		streams = a.streams
		a.streams = make(map[uint64]*drpcstream.Stream)
		a.latestID = 0
	}()

	for _, s := range streams {
		e := err
		if errors.Is(err, io.EOF) {
			e = context.Canceled
			if s.Kind() == drpc.StreamKindClient {
				e = drpc.ClosedError.New("connection closed")
			}
		}
		s.Cancel(e)
	}
}
