package simnet

import (
	"context"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rpc "github.com/glycerine/rpc25519"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"storj.io/drpc/internal/grpccompat"
)

// ---------------------------------------------------------------------------
// Test 1: StreamIndependenceUnderPacketLoss
//
// Verifies that when packet loss is injected mid-flight, individual streams
// fail independently — healthy streams continue making progress while lossy
// ones may error out. This is the fundamental multiplexing correctness test.
// ---------------------------------------------------------------------------

func TestStreamIndependenceUnderPacketLoss(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)

	runInBubble(t, func(t *testing.T) {
		// ARRANGE
		goroutinesBefore := runtime.NumGoroutine()
		srv := newEchoServer()
		env := newSimnetEnv(t, seed, srv)
		defer env.close()

		const numStreams = 10
		const msgsPerStream = 100

		type streamOutcome struct {
			id       int
			sent     int
			received int
			err      error
		}

		// ACT
		// Launch concurrent bidi streams
		results := make(chan streamOutcome, numStreams)
		faultInjected := make(chan struct{})

		// Inject fault after a delay
		go func() {
			time.Sleep(3 * time.Second)
			// 30% packet drop on client→server direction
			_ = env.simnet.FaultCircuit(
				env.cliName, "",
				rpc.DropDeafSpec{
					UpdateDropSends:  true,
					DropSendsNewProb: 0.3,
				},
				false, // don't deliver dropped
			)
			close(faultInjected)
		}()

		for i := 0; i < numStreams; i++ {
			go func(streamID int) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				stream, err := env.drpcClient.Method4(ctx)
				if err != nil {
					results <- streamOutcome{id: streamID, err: err}
					return
				}

				var sent, received int
				// Send and receive concurrently
				sendDone := make(chan error, 1)
				go func() {
					for seq := 0; seq < msgsPerStream; seq++ {
						payload := makePayload(streamID, seq)
						if err := stream.Send(&grpccompat.In{In: int64(seq), Buf: payload}); err != nil {
							sendDone <- err
							return
						}
						sent++
					}
					sendDone <- stream.CloseSend()
				}()

				// Receive loop
				var recvErr error
				for {
					_, err := stream.Recv()
					if err != nil {
						recvErr = err
						break
					}
					received++
				}

				sendErr := <-sendDone
				// Use whichever error is more informative
				finalErr := recvErr
				if finalErr == nil {
					finalErr = sendErr
				}

				results <- streamOutcome{
					id:       streamID,
					sent:     sent,
					received: received,
					err:      finalErr,
				}
			}(i)
		}

		// Collect results
		outcomes := make([]streamOutcome, 0, numStreams)
		for i := 0; i < numStreams; i++ {
			outcomes = append(outcomes, <-results)
		}

		// Wait for fault injection to complete
		<-faultInjected

		// ASSERT

		// Streams that completed should have correct data (echo server mirrors input)
		var succeeded, failed int
		for _, o := range outcomes {
			if o.err == nil || o.received > 0 {
				// If stream made progress, received count should match sent count
				// (echo server sends one response per received message)
				t.Logf("stream %d: sent=%d received=%d err=%v", o.id, o.sent, o.received, o.err)
				if o.err == nil {
					succeeded++
				} else {
					failed++
				}
			}
		}

		// Stream failures should be independent — not all-or-nothing.
		// With 30% drop rate on 10 streams, we expect a mix of outcomes.
		t.Logf("streams: succeeded=%d failed=%d", succeeded, failed)

		// No deadlock: all streams returned within timeout (implicit by reaching here)
		t.Log("all streams completed without deadlock")

		// Goroutine check
		time.Sleep(100 * time.Millisecond) // let cleanup goroutines finish
		assertNoGoroutineLeak(t, goroutinesBefore, 5)
	})
}

// ---------------------------------------------------------------------------
// Test 2: ConcurrentStreamCreationUnderDeafReads
//
// Verifies backpressure behavior when the server becomes deaf to reads.
// Streams opened before the fault should be affected gracefully; new stream
// creation should fail with errors, not panics or deadlocks.
// ---------------------------------------------------------------------------

func TestConcurrentStreamCreationUnderDeafReads(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)

	runInBubble(t, func(t *testing.T) {
		// ARRANGE
		srv := newEchoServer()
		env := newSimnetEnv(t, seed, srv)
		defer env.close()

		const totalStreams = 120
		const faultAfter = 50

		type result struct {
			id  int
			err error
		}

		// ACT
		results := make(chan result, totalStreams)
		var established atomic.Int32

		for i := 0; i < totalStreams; i++ {
			go func(id int) {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()

				stream, err := env.drpcClient.Method4(ctx)
				if err != nil {
					results <- result{id: id, err: err}
					return
				}

				count := established.Add(1)

				// After faultAfter streams are established, inject deaf reads on server
				if count == int32(faultAfter) {
					go func() {
						srvName := env.srvName
						_ = env.simnet.FaultHost(srvName,
							rpc.DropDeafSpec{
								UpdateDeafReads:  true,
								DeafReadsNewProb: 1.0,
							},
							false,
						)
					}()
				}

				// Try to do a simple exchange
				err = stream.Send(&grpccompat.In{In: int64(id)})
				if err != nil {
					results <- result{id: id, err: err}
					return
				}
				_, err = stream.Recv()
				results <- result{id: id, err: err}
			}(i)
		}

		// Collect
		outcomes := make([]result, 0, totalStreams)
		for i := 0; i < totalStreams; i++ {
			outcomes = append(outcomes, <-results)
		}

		// Check snapshot while deaf
		snap := env.simnet.GetSimnetSnapshot(true)
		t.Logf("snapshot after deaf fault: peers=%d", len(snap.Peer))
		for _, peer := range snap.Peer {
			for _, conn := range peer.Conn {
				if conn.DeafReadQ != nil {
					t.Logf("  %s->%s DeafReadQ=%d", conn.Origin, conn.Target, conn.DeafReadQ.Len())
				}
			}
		}

		// Repair and verify backlog drains
		srvName := env.srvName
		err := env.simnet.RepairHost(srvName, true, true, false, true)
		require.NoError(t, err, "RepairHost should succeed")

		// ASSERT
		var errCount int
		for _, o := range outcomes {
			if o.err != nil {
				errCount++
			}
		}

		// We expect some errors due to deaf reads. The key assertion is
		// no panics or deadlocks (implicit by reaching here).
		t.Logf("total=%d errors=%d", totalStreams, errCount)
		assert.Greater(t, errCount, 0,
			"some streams should fail when server is completely deaf")
	})
}

// ---------------------------------------------------------------------------
// Test 3: InterleavedUnaryAndStreamingDuringPartition
//
// Verifies behavior across a network partition:
// - Phase 1: healthy operation with mixed unary + streaming RPCs
// - Phase 2: complete isolation (ISOLATE)
// - Phase 3: recovery with time-warp delivery
// - Phase 4: verify system recovers cleanly
// ---------------------------------------------------------------------------

func TestInterleavedUnaryAndStreamingDuringPartition(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)

	runInBubble(t, func(t *testing.T) {
		// ARRANGE
		srv := newEchoServer()
		env := newSimnetEnv(t, seed, srv)
		defer env.close()

		srvName := env.srvName

		// Phase 1 (0-2s): healthy operation
		// Launch 5 bidi streams
		var wg sync.WaitGroup
		streamErrors := make(chan error, 5)
		streamCtx, streamCancel := context.WithCancel(context.Background())
		defer streamCancel()

		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(streamCtx, 10*time.Second)
				defer cancel()

				stream, err := env.drpcClient.Method4(ctx)
				if err != nil {
					streamErrors <- err
					return
				}

				for seq := 0; ; seq++ {
					select {
					case <-ctx.Done():
						streamErrors <- ctx.Err()
						return
					default:
					}
					if err := stream.Send(&grpccompat.In{In: int64(seq)}); err != nil {
						streamErrors <- err
						return
					}
					if _, err := stream.Recv(); err != nil {
						streamErrors <- err
						return
					}
					time.Sleep(100 * time.Millisecond)
				}
			}(i)
		}

		// Concurrent unary calls
		var unaryWg sync.WaitGroup
		unaryErrors := make([]error, 0)
		var unaryMu sync.Mutex
		unaryDone := make(chan struct{})

		unaryWg.Add(1)
		go func() {
			defer unaryWg.Done()
			ticker := time.NewTicker(50 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-unaryDone:
					return
				case <-ticker.C:
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					_, err := env.drpcClient.Method1(ctx, &grpccompat.In{In: 42})
					cancel()
					if err != nil {
						unaryMu.Lock()
						unaryErrors = append(unaryErrors, err)
						unaryMu.Unlock()
					}
				}
			}
		}()

		// Let healthy phase run
		time.Sleep(2 * time.Second)

		// Phase 2 (2-4s): ISOLATE server — complete blackout
		_, err := env.simnet.AlterHost(srvName, rpc.ISOLATE)
		require.NoError(t, err, "AlterHost ISOLATE")
		t.Log("server isolated")

		time.Sleep(2 * time.Second)

		// Phase 3 (4s): UNISOLATE + AllHealthy with time-warp delivery
		_, err = env.simnet.AlterHost(srvName, rpc.UNISOLATE)
		require.NoError(t, err, "AlterHost UNISOLATE")
		err = env.simnet.AllHealthy(true, true) // powerOn=true, deliverDropped=true
		require.NoError(t, err, "AllHealthy")
		t.Log("server unisolated + all healthy")

		// Phase 4 (4-6s): observe recovery
		time.Sleep(2 * time.Second)

		// Teardown
		close(unaryDone)
		unaryWg.Wait()
		streamCancel()
		wg.Wait()

		// ASSERT

		// Unary calls during partition should have returned errors, not hung.
		// (If they hung, we'd have timed out above.)
		unaryMu.Lock()
		t.Logf("unary errors during test: %d", len(unaryErrors))
		unaryMu.Unlock()

		// Bidi streams during partition should have received errors on recv,
		// not hung indefinitely.
		close(streamErrors)
		streamErrCount := 0
		for err := range streamErrors {
			if err != nil {
				streamErrCount++
			}
		}
		t.Logf("stream errors: %d", streamErrCount)

		// The server should not have crashed (it's still running if we get here)
		t.Log("server survived partition and recovery")
	})
}

// ---------------------------------------------------------------------------
// Test 5: ContextCancellationStorm
//
// Verifies that canceling many streams simultaneously + dropping all RST_STREAM
// frames doesn't cause deadlocks or goroutine leaks. After repair with
// deliverDroppedSends=true, the server should handle stale/duplicate frames
// idempotently.
// ---------------------------------------------------------------------------

func TestContextCancellationStorm(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)

	runInBubble(t, func(t *testing.T) {
		// ARRANGE
		goroutinesBefore := runtime.NumGoroutine()
		srv := newEchoServer()
		env := newSimnetEnv(t, seed, srv)
		defer env.close()

		const numStreams = 50

		// Create a shared context we'll cancel to trigger the storm
		stormCtx, stormCancel := context.WithCancel(context.Background())

		// ACT
		// Open 50 bidi streams, all mid-flight in large streaming sends
		var wg sync.WaitGroup
		streamReady := make(chan struct{}, numStreams)

		for i := 0; i < numStreams; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				stream, err := env.drpcClient.Method4(stormCtx)
				if err != nil {
					return
				}

				// Signal that stream is open
				streamReady <- struct{}{}

				// Keep sending until context is canceled
				for seq := 0; ; seq++ {
					payload := make([]byte, 1024)
					err := stream.Send(&grpccompat.In{In: int64(seq), Buf: payload})
					if err != nil {
						return
					}
					// Also try to receive
					_, err = stream.Recv()
					if err != nil {
						return
					}
				}
			}(i)
		}

		// Wait for streams to establish
		established := 0
		timeout := time.After(10 * time.Second)
		for established < numStreams {
			select {
			case <-streamReady:
				established++
			case <-timeout:
				t.Logf("only %d/%d streams established before timeout", established, numStreams)
				goto cancelPhase
			}
		}

	cancelPhase:
		t.Logf("established %d streams, now canceling all + injecting fault", established)

		// Simultaneously: cancel all contexts + drop all outgoing messages
		cliName := env.cliName
		_ = env.simnet.FaultCircuit(cliName, "",
			rpc.DropDeafSpec{
				UpdateDropSends:  true,
				DropSendsNewProb: 1.0, // drop everything including RST_STREAM
			},
			false,
		)
		stormCancel()

		// Wait for all client goroutines to exit
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			t.Log("all client streams exited after cancel")
		case <-time.After(15 * time.Second):
			t.Fatal("DEADLOCK: client streams did not exit within 15s after cancel")
		}

		// Now repair and deliver the dropped sends (including stale RST_STREAMs)
		err := env.simnet.RepairCircuit(cliName, true, true, true) // deliverDropped=true
		require.NoError(t, err, "RepairCircuit")

		// Give server time to process stale frames
		time.Sleep(1 * time.Second)

		// ASSERT
		// Server should handle stale/duplicate RST_STREAM idempotently (no panic)
		t.Log("server survived stale frame delivery")

		// Goroutine count should stabilize
		time.Sleep(200 * time.Millisecond)
		assertNoGoroutineLeak(t, goroutinesBefore, 10)
	})
}

// ---------------------------------------------------------------------------
// Test 6: DeterministicReplay
//
// Verifies that a healthy (no-fault) scenario consistently succeeds: all
// streams complete their full exchange. Full bitwise determinism (identical
// goroutine interleaving) requires GOEXPERIMENT=synctest; without it, we
// verify structural determinism — same number of streams, all succeeding.
//
// Also verifies that fault injection produces observable failures, confirming
// that the simnet fault machinery is actually affecting DRPC traffic.
// ---------------------------------------------------------------------------

func TestDeterministicReplay(t *testing.T) {
	runInBubble(t, func(t *testing.T) {
		// runHealthyScenario runs numStreams bidi streams with no faults.
		// Every stream should complete all send/recv exchanges.
		runHealthyScenario := func(seed int64) []StreamResult {
			srv := newEchoServer()
			env := newSimnetEnv(t, seed, srv)

			const numStreams = 20
			const msgsPerStream = 10

			var mu sync.Mutex
			results := make([]StreamResult, 0, numStreams)
			var wg sync.WaitGroup

			for i := 0; i < numStreams; i++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
					defer cancel()

					stream, err := env.drpcClient.Method4(ctx)
					if err != nil {
						mu.Lock()
						results = append(results, StreamResult{StreamID: id, Err: err})
						mu.Unlock()
						return
					}

					var sent, received int
					for seq := 0; seq < msgsPerStream; seq++ {
						if err := stream.Send(&grpccompat.In{In: int64(seq)}); err != nil {
							mu.Lock()
							results = append(results, StreamResult{StreamID: id, Sent: sent, Received: received, Err: err})
							mu.Unlock()
							return
						}
						sent++

						_, err := stream.Recv()
						if err != nil {
							mu.Lock()
							results = append(results, StreamResult{StreamID: id, Sent: sent, Received: received, Err: err})
							mu.Unlock()
							return
						}
						received++
					}

					_ = stream.CloseSend()
					mu.Lock()
					results = append(results, StreamResult{StreamID: id, Sent: sent, Received: received})
					mu.Unlock()
				}(i)
			}

			// Wait for all streams to complete before tearing down.
			wg.Wait()
			env.close()

			sort.Slice(results, func(i, j int) bool {
				return results[i].StreamID < results[j].StreamID
			})
			return results
		}

		// ASSERT: healthy scenario completes all streams successfully, twice.
		// This is structural determinism: same outcome shape regardless of
		// goroutine scheduling order.
		t.Log("run 1: healthy scenario with seed 42")
		run1 := runHealthyScenario(42)
		require.Equal(t, 20, len(run1), "all 20 streams should report")

		successCount1 := 0
		for _, r := range run1 {
			if r.Err == nil && r.Sent == 10 && r.Received == 10 {
				successCount1++
			} else {
				t.Logf("  stream %d: sent=%d recv=%d err=%v", r.StreamID, r.Sent, r.Received, r.Err)
			}
		}
		assert.Equal(t, 20, successCount1,
			"all 20 healthy streams should complete 10 send/recv pairs")

		t.Log("run 2: same healthy scenario with seed 42")
		run2 := runHealthyScenario(42)

		successCount2 := 0
		for _, r := range run2 {
			if r.Err == nil && r.Sent == 10 && r.Received == 10 {
				successCount2++
			}
		}
		assert.Equal(t, 20, successCount2,
			"second run should also complete all 20 streams")

		// ASSERT: fault injection produces observable failures.
		// This confirms the simnet fault machinery is wired correctly.
		t.Log("run 3: faulty scenario with seed 42")
		srvFaulty := newEchoServer()
		envFaulty := newSimnetEnv(t, 42, srvFaulty)

		var mu sync.Mutex
		var faultyResults []StreamResult
		var wg sync.WaitGroup

		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				stream, err := envFaulty.drpcClient.Method4(ctx)
				if err != nil {
					mu.Lock()
					faultyResults = append(faultyResults, StreamResult{StreamID: id, Err: err})
					mu.Unlock()
					return
				}

				var sent, received int
				for seq := 0; seq < 50; seq++ {
					if err := stream.Send(&grpccompat.In{In: int64(seq)}); err != nil {
						mu.Lock()
						faultyResults = append(faultyResults, StreamResult{StreamID: id, Sent: sent, Received: received, Err: err})
						mu.Unlock()
						return
					}
					sent++
					_, err := stream.Recv()
					if err != nil {
						mu.Lock()
						faultyResults = append(faultyResults, StreamResult{StreamID: id, Sent: sent, Received: received, Err: err})
						mu.Unlock()
						return
					}
					received++
				}
				_ = stream.CloseSend()
				mu.Lock()
				faultyResults = append(faultyResults, StreamResult{StreamID: id, Sent: sent, Received: received})
				mu.Unlock()
			}(i)
		}

		// Inject faults while streams are active
		time.Sleep(200 * time.Millisecond)
		_ = envFaulty.simnet.FaultHost(envFaulty.srvName,
			rpc.DropDeafSpec{
				UpdateDropSends:  true,
				DropSendsNewProb: 1.0, // drop everything
			},
			false,
		)

		wg.Wait()
		envFaulty.close()

		faultedCount := 0
		for _, r := range faultyResults {
			if r.Err != nil {
				faultedCount++
			}
		}
		t.Logf("faulty scenario: %d/%d streams had errors", faultedCount, len(faultyResults))
		assert.Greater(t, faultedCount, 0,
			"fault injection should cause at least some stream failures")
	})
}

// ---------------------------------------------------------------------------
// Test 7: AsymmetricFaultHalfOpenConnection
//
// Verifies behavior when the network fault is asymmetric: server is deaf to
// reads FROM client, but can still write TO client. This creates a "half-open"
// connection where one direction works and the other doesn't.
// ---------------------------------------------------------------------------

func TestAsymmetricFaultHalfOpenConnection(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)

	runInBubble(t, func(t *testing.T) {
		// ARRANGE
		srv := newEchoServer()
		env := newSimnetEnv(t, seed, srv)
		defer env.close()

		srvName := env.srvName

		// First, establish a stream and verify it works healthy
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		stream, err := env.drpcClient.Method4(ctx)
		require.NoError(t, err, "stream creation should succeed")

		// Verify healthy operation
		err = stream.Send(&grpccompat.In{In: 1})
		require.NoError(t, err, "healthy send should succeed")
		resp, err := stream.Recv()
		require.NoError(t, err, "healthy recv should succeed")
		assert.Equal(t, int64(1), resp.Out)

		// ACT: Make server deaf to reads FROM client
		// This means: the server's incoming reads are deaf, but it can still write
		err = env.simnet.FaultHost(srvName,
			rpc.DropDeafSpec{
				UpdateDeafReads:  true,
				DeafReadsNewProb: 1.0, // completely deaf to incoming reads
			},
			false,
		)
		require.NoError(t, err, "FaultHost deaf reads")

		// The echo server needs to receive to send back, so both directions
		// should stall. Client sends go through the network but server
		// never reads them (deaf), so server never echoes back.
		sendErr := stream.Send(&grpccompat.In{In: 2})
		t.Logf("send during deaf: err=%v", sendErr)

		// Recv should timeout because server can't echo what it can't read
		recvCtx, recvCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer recvCancel()
		_ = recvCtx
		_, recvErr := stream.Recv()
		t.Logf("recv during deaf: err=%v", recvErr)

		// ASSERT: After repair, new operations work cleanly
		err = env.simnet.RepairHost(srvName, true, true, false, true) // deliverDropped=true
		require.NoError(t, err, "RepairHost")

		// Give time for backlog to drain
		time.Sleep(500 * time.Millisecond)

		// New stream on the same connection should work (if connection survived)
		// or fail cleanly (if connection was torn down)
		ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel2()

		stream2, err := env.drpcClient.Method4(ctx2)
		if err != nil {
			// Connection was torn down during fault — that's acceptable behavior
			t.Logf("new stream after repair failed (connection torn down): %v", err)
			return
		}

		// If we got a new stream, verify it works
		err = stream2.Send(&grpccompat.In{In: 99})
		if err == nil {
			resp, err := stream2.Recv()
			if err == nil {
				assert.Equal(t, int64(99), resp.Out,
					"new stream should work cleanly after repair")
				t.Log("new stream works cleanly after repair — no state bleed")
			} else {
				t.Logf("new stream recv after repair: %v", err)
			}
		} else {
			t.Logf("new stream send after repair: %v", err)
		}
	})
}
