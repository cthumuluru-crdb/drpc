// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package drpcmanager

import (
	"sync"

	"storj.io/drpc/drpcstream"
)

type activeStreams struct {
	mu          sync.RWMutex
	cond        sync.Cond
	streams     map[uint64]*drpcstream.Stream
	maxStreamID uint64
	closed      bool
}

func (sb *activeStreams) init() {
	sb.cond.L = &sb.mu
	sb.streams = make(map[uint64]*drpcstream.Stream)
}

func (sb *activeStreams) Close() {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	sb.streams = nil
	sb.closed = true
	sb.cond.Broadcast()
}

func (sb *activeStreams) GetMaxStream() *drpcstream.Stream {
	sb.mu.RLock()
	defer sb.mu.RUnlock()

	return sb.streams[sb.maxStreamID]
}

func (sb *activeStreams) Register(stream *drpcstream.Stream) {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	if sb.closed {
		return
	}
	sb.streams[stream.ID()] = stream

	// TODO(chandrat) with multiplexing we don't need this.
	if sb.maxStreamID < stream.ID() {
		sb.maxStreamID = stream.ID()
	}
	sb.cond.Broadcast()
}

func (sb *activeStreams) Unregister(sid uint64) {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	delete(sb.streams, sid)
	sb.cond.Broadcast()
}

func (sb *activeStreams) Wait(sid uint64) bool {
	sb.mu.Lock()
	defer sb.mu.Unlock()

	for !sb.closed && sb.maxStreamID == sid {
		sb.cond.Wait()
	}
	return !sb.closed
}
