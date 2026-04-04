package simnet

import (
	"fmt"
	"runtime"
	"sort"
	"testing"

	rpc "github.com/glycerine/rpc25519"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// StreamResult captures the outcome of a single stream in a test scenario.
type StreamResult struct {
	StreamID int
	Sent     int
	Received int
	Err      error
}

// assertNoGoroutineLeak verifies the goroutine count hasn't grown beyond tolerance.
// This catches leaked goroutines from streams that weren't properly cleaned up.
func assertNoGoroutineLeak(t *testing.T, before int, tolerance int) {
	t.Helper()
	after := runtime.NumGoroutine()
	delta := after - before
	if delta > tolerance {
		t.Errorf("goroutine leak detected: before=%d after=%d delta=%d (tolerance=%d)",
			before, after, delta, tolerance)
	}
}

// assertSnapshotClean verifies all queue lengths are zero after test cleanup.
// Non-zero queues indicate messages stuck in the network — either leaked
// or not properly drained during teardown.
func assertSnapshotClean(t *testing.T, snap *rpc.SimnetSnapshot) {
	t.Helper()
	if snap == nil {
		t.Log("snapshot is nil, skipping queue check")
		return
	}

	for _, peer := range snap.Peer {
		for _, conn := range peer.Conn {
			if conn.DroppedSendQ != nil && conn.DroppedSendQ.Len() > 0 {
				t.Errorf("peer %s conn %s->%s: DroppedSendQ not empty (%d items)",
					peer.Name, conn.Origin, conn.Target, conn.DroppedSendQ.Len())
			}
			if conn.DeafReadQ != nil && conn.DeafReadQ.Len() > 0 {
				t.Errorf("peer %s conn %s->%s: DeafReadQ not empty (%d items)",
					peer.Name, conn.Origin, conn.Target, conn.DeafReadQ.Len())
			}
			if conn.ReadQ != nil && conn.ReadQ.Len() > 0 {
				t.Errorf("peer %s conn %s->%s: ReadQ not empty (%d items)",
					peer.Name, conn.Origin, conn.Target, conn.ReadQ.Len())
			}
			if conn.PreArrQ != nil && conn.PreArrQ.Len() > 0 {
				t.Errorf("peer %s conn %s->%s: PreArrQ not empty (%d items)",
					peer.Name, conn.Origin, conn.Target, conn.PreArrQ.Len())
			}
		}
	}
}

// assertDeterministic runs the same scenario twice with the same seed and verifies
// the outcomes match. This validates that the simnet scheduler + DRPC combination
// is fully deterministic for a given seed.
func assertDeterministic(t *testing.T, seed int64, fn func(int64) []StreamResult) {
	t.Helper()

	run1 := fn(seed)
	run2 := fn(seed)

	require.Equal(t, len(run1), len(run2), "run count mismatch for seed %d", seed)

	// Sort by StreamID for stable comparison
	sort.Slice(run1, func(i, j int) bool { return run1[i].StreamID < run1[j].StreamID })
	sort.Slice(run2, func(i, j int) bool { return run2[i].StreamID < run2[j].StreamID })

	for i := range run1 {
		r1, r2 := run1[i], run2[i]
		assert.Equal(t, r1.StreamID, r2.StreamID, "stream ID mismatch at index %d", i)
		assert.Equal(t, r1.Sent, r2.Sent, "sent count mismatch for stream %d", r1.StreamID)
		assert.Equal(t, r1.Received, r2.Received, "received count mismatch for stream %d", r1.StreamID)

		// Both runs should either succeed or fail
		if (r1.Err == nil) != (r2.Err == nil) {
			t.Errorf("stream %d: error mismatch: run1=%v run2=%v", r1.StreamID, r1.Err, r2.Err)
		}
	}
}

// assertNoDataCorruption verifies that every received message matches what was sent.
// This catches byte-level corruption from interleaved writes on shared connections.
func assertNoDataCorruption(t *testing.T, sent, received [][]byte) {
	t.Helper()
	for i, r := range received {
		if i >= len(sent) {
			t.Errorf("received extra message at index %d", i)
			continue
		}
		assert.Equal(t, sent[i], r, "data corruption at message index %d", i)
	}
}

// makePayload creates a deterministic payload of the given size for a stream/sequence.
func makePayload(streamID, seq int) []byte {
	return []byte(fmt.Sprintf("stream-%d-seq-%d", streamID, seq))
}
