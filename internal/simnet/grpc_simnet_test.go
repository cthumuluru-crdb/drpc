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
// gRPC Baseline: StreamIndependenceUnderPacketLoss
//
// Identical scenario to TestStreamIndependenceUnderPacketLoss but using gRPC.
// If gRPC passes but DRPC fails → DRPC multiplexing bug.
// If both fail the same way → simnet or test infrastructure issue.
// ---------------------------------------------------------------------------

func TestGRPCBaselineStreamIndependence(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)

	runInBubble(t, func(t *testing.T) {
		goroutinesBefore := runtime.NumGoroutine()
		srv := newGRPCEchoServer()
		env := newGRPCSimnetEnv(t, seed, srv)
		defer env.close()

		const numStreams = 10
		const msgsPerStream = 100

		type streamOutcome struct {
			id       int
			sent     int
			received int
			err      error
		}

		results := make(chan streamOutcome, numStreams)
		faultInjected := make(chan struct{})

		go func() {
			time.Sleep(3 * time.Second)
			_ = env.simnet.FaultCircuit(
				env.cliName, "",
				rpc.DropDeafSpec{
					UpdateDropSends:  true,
					DropSendsNewProb: 0.3,
				},
				false,
			)
			close(faultInjected)
		}()

		for i := 0; i < numStreams; i++ {
			go func(streamID int) {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				stream, err := env.grpcClient.Method4(ctx)
				if err != nil {
					results <- streamOutcome{id: streamID, err: err}
					return
				}

				var sent, received int
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

		outcomes := make([]streamOutcome, 0, numStreams)
		for i := 0; i < numStreams; i++ {
			outcomes = append(outcomes, <-results)
		}
		<-faultInjected

		var succeeded, failed int
		for _, o := range outcomes {
			t.Logf("gRPC stream %d: sent=%d received=%d err=%v", o.id, o.sent, o.received, o.err)
			// EOF after full exchange is normal stream completion
			if o.sent == msgsPerStream && o.received == msgsPerStream {
				succeeded++
			} else {
				failed++
			}
		}
		t.Logf("gRPC streams: succeeded=%d failed=%d", succeeded, failed)
		t.Log("all gRPC streams completed without deadlock")

		time.Sleep(500 * time.Millisecond)
		assertNoGoroutineLeak(t, goroutinesBefore, 15) // gRPC uses more goroutines
	})
}

// ---------------------------------------------------------------------------
// gRPC Baseline: DeterministicReplay
//
// Identical healthy scenario to TestDeterministicReplay but using gRPC.
// Validates that the test infrastructure (simnet wiring, echo server) works
// correctly independent of DRPC.
// ---------------------------------------------------------------------------

func TestGRPCBaselineDeterministicReplay(t *testing.T) {
	runInBubble(t, func(t *testing.T) {
		runHealthyScenario := func(seed int64) []StreamResult {
			srv := newGRPCEchoServer()
			env := newGRPCSimnetEnv(t, seed, srv)

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

					stream, err := env.grpcClient.Method4(ctx)
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

			wg.Wait()
			env.close()

			sort.Slice(results, func(i, j int) bool {
				return results[i].StreamID < results[j].StreamID
			})
			return results
		}

		// Run 1: healthy gRPC scenario
		t.Log("gRPC run 1: healthy scenario with seed 42")
		run1 := runHealthyScenario(42)
		require.Equal(t, 20, len(run1), "all 20 gRPC streams should report")

		successCount1 := 0
		for _, r := range run1 {
			if r.Err == nil && r.Sent == 10 && r.Received == 10 {
				successCount1++
			} else {
				t.Logf("gRPC stream %d: sent=%d recv=%d err=%v", r.StreamID, r.Sent, r.Received, r.Err)
			}
		}
		assert.Equal(t, 20, successCount1,
			"all 20 healthy gRPC streams should complete 10 send/recv pairs")

		// Run 2: same seed
		t.Log("gRPC run 2: same healthy scenario with seed 42")
		run2 := runHealthyScenario(42)

		successCount2 := 0
		for _, r := range run2 {
			if r.Err == nil && r.Sent == 10 && r.Received == 10 {
				successCount2++
			}
		}
		assert.Equal(t, 20, successCount2,
			"second gRPC run should also complete all 20 streams")

		// Run 3: faulty scenario — confirm fault injection affects gRPC too
		t.Log("gRPC run 3: faulty scenario with seed 42")
		srvFaulty := newGRPCEchoServer()
		envFaulty := newGRPCSimnetEnv(t, 42, srvFaulty)

		var mu sync.Mutex
		var faultyResults []StreamResult
		var wg sync.WaitGroup

		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				stream, err := envFaulty.grpcClient.Method4(ctx)
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

		time.Sleep(200 * time.Millisecond)
		_ = envFaulty.simnet.FaultHost(envFaulty.srvName,
			rpc.DropDeafSpec{
				UpdateDropSends:  true,
				DropSendsNewProb: 1.0,
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
		t.Logf("gRPC faulty scenario: %d/%d streams had errors", faultedCount, len(faultyResults))
		assert.Greater(t, faultedCount, 0,
			"fault injection should cause at least some gRPC stream failures")
	})
}

// ---------------------------------------------------------------------------
// gRPC Baseline: InterleavedUnaryAndStreamingDuringPartition
//
// Verifies gRPC behavior across ISOLATE/UNISOLATE partition.
// ---------------------------------------------------------------------------

func TestGRPCBaselinePartition(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)

	runInBubble(t, func(t *testing.T) {
		srv := newGRPCEchoServer()
		env := newGRPCSimnetEnv(t, seed, srv)
		defer env.close()

		// Phase 1: healthy bidi streams
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

				stream, err := env.grpcClient.Method4(ctx)
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
		var unaryErrors []error
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
					_, err := env.grpcClient.Method1(ctx, &grpccompat.In{In: 42})
					cancel()
					if err != nil {
						unaryMu.Lock()
						unaryErrors = append(unaryErrors, err)
						unaryMu.Unlock()
					}
				}
			}
		}()

		time.Sleep(2 * time.Second)

		// Phase 2: ISOLATE
		_, err := env.simnet.AlterHost(env.srvName, rpc.ISOLATE)
		require.NoError(t, err, "AlterHost ISOLATE")
		t.Log("gRPC server isolated")

		time.Sleep(2 * time.Second)

		// Phase 3: UNISOLATE + AllHealthy
		_, err = env.simnet.AlterHost(env.srvName, rpc.UNISOLATE)
		require.NoError(t, err, "AlterHost UNISOLATE")
		err = env.simnet.AllHealthy(true, true)
		require.NoError(t, err, "AllHealthy")
		t.Log("gRPC server unisolated + all healthy")

		// Phase 4: observe recovery
		time.Sleep(2 * time.Second)

		close(unaryDone)
		unaryWg.Wait()
		streamCancel()
		wg.Wait()

		unaryMu.Lock()
		t.Logf("gRPC unary errors during test: %d", len(unaryErrors))
		unaryMu.Unlock()

		close(streamErrors)
		streamErrCount := 0
		for err := range streamErrors {
			if err != nil {
				streamErrCount++
			}
		}
		t.Logf("gRPC stream errors: %d", streamErrCount)
		t.Log("gRPC server survived partition and recovery")
	})
}

// ---------------------------------------------------------------------------
// gRPC Baseline: ConcurrentStreamCreationUnderDeafReads
//
// Identical scenario to TestConcurrentStreamCreationUnderDeafReads but
// using gRPC. Verifies backpressure behavior when the server becomes
// deaf to reads.
// ---------------------------------------------------------------------------

func TestGRPCBaselineDeafReads(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)

	runInBubble(t, func(t *testing.T) {
		srv := newGRPCEchoServer()
		env := newGRPCSimnetEnv(t, seed, srv)
		defer env.close()

		const totalStreams = 120
		const faultAfter = 50

		type result struct {
			id  int
			err error
		}

		results := make(chan result, totalStreams)
		var established atomic.Int32

		for i := 0; i < totalStreams; i++ {
			go func(id int) {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()

				stream, err := env.grpcClient.Method4(ctx)
				if err != nil {
					results <- result{id: id, err: err}
					return
				}

				count := established.Add(1)

				if count == int32(faultAfter) {
					go func() {
						_ = env.simnet.FaultHost(env.srvName,
							rpc.DropDeafSpec{
								UpdateDeafReads:  true,
								DeafReadsNewProb: 1.0,
							},
							false,
						)
					}()
				}

				err = stream.Send(&grpccompat.In{In: int64(id)})
				if err != nil {
					results <- result{id: id, err: err}
					return
				}
				_, err = stream.Recv()
				results <- result{id: id, err: err}
			}(i)
		}

		outcomes := make([]result, 0, totalStreams)
		for i := 0; i < totalStreams; i++ {
			outcomes = append(outcomes, <-results)
		}

		snap := env.simnet.GetSimnetSnapshot(true)
		t.Logf("gRPC snapshot after deaf fault: peers=%d", len(snap.Peer))
		for _, peer := range snap.Peer {
			for _, conn := range peer.Conn {
				if conn.DeafReadQ != nil {
					t.Logf("  %s->%s DeafReadQ=%d", conn.Origin, conn.Target, conn.DeafReadQ.Len())
				}
			}
		}

		err := env.simnet.RepairHost(env.srvName, true, true, false, true)
		require.NoError(t, err, "RepairHost should succeed")

		var errCount int
		for _, o := range outcomes {
			if o.err != nil {
				errCount++
			}
		}

		t.Logf("gRPC total=%d errors=%d", totalStreams, errCount)
		assert.Greater(t, errCount, 0,
			"some gRPC streams should fail when server is completely deaf")
	})
}

// ---------------------------------------------------------------------------
// gRPC Baseline: ContextCancellationStorm
//
// Identical scenario to TestContextCancellationStorm but using gRPC.
// Cancel many streams simultaneously + drop all outgoing messages.
// ---------------------------------------------------------------------------

func TestGRPCBaselineCancellationStorm(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)

	runInBubble(t, func(t *testing.T) {
		goroutinesBefore := runtime.NumGoroutine()
		srv := newGRPCEchoServer()
		env := newGRPCSimnetEnv(t, seed, srv)
		defer env.close()

		const numStreams = 50

		stormCtx, stormCancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup
		streamReady := make(chan struct{}, numStreams)

		for i := 0; i < numStreams; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				stream, err := env.grpcClient.Method4(stormCtx)
				if err != nil {
					return
				}

				streamReady <- struct{}{}

				for seq := 0; ; seq++ {
					payload := make([]byte, 1024)
					err := stream.Send(&grpccompat.In{In: int64(seq), Buf: payload})
					if err != nil {
						return
					}
					_, err = stream.Recv()
					if err != nil {
						return
					}
				}
			}(i)
		}

		established := 0
		timeout := time.After(10 * time.Second)
		for established < numStreams {
			select {
			case <-streamReady:
				established++
			case <-timeout:
				t.Logf("only %d/%d gRPC streams established before timeout", established, numStreams)
				goto cancelPhase
			}
		}

	cancelPhase:
		t.Logf("established %d gRPC streams, now canceling all + injecting fault", established)

		_ = env.simnet.FaultCircuit(env.cliName, "",
			rpc.DropDeafSpec{
				UpdateDropSends:  true,
				DropSendsNewProb: 1.0,
			},
			false,
		)
		stormCancel()

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			t.Log("all gRPC client streams exited after cancel")
		case <-time.After(15 * time.Second):
			t.Fatal("DEADLOCK: gRPC client streams did not exit within 15s after cancel")
		}

		err := env.simnet.RepairCircuit(env.cliName, true, true, true)
		require.NoError(t, err, "RepairCircuit")

		time.Sleep(1 * time.Second)
		t.Log("gRPC server survived stale frame delivery")

		time.Sleep(500 * time.Millisecond)
		assertNoGoroutineLeak(t, goroutinesBefore, 15)
	})
}

// ---------------------------------------------------------------------------
// gRPC Baseline: AsymmetricFaultHalfOpenConnection
//
// Identical scenario to TestAsymmetricFaultHalfOpenConnection but using gRPC.
// Server is deaf to reads FROM client, creating a half-open connection.
// ---------------------------------------------------------------------------

func TestGRPCBaselineAsymmetricFault(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)

	runInBubble(t, func(t *testing.T) {
		srv := newGRPCEchoServer()
		env := newGRPCSimnetEnv(t, seed, srv)
		defer env.close()

		// Establish a stream and verify healthy operation.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		stream, err := env.grpcClient.Method4(ctx)
		require.NoError(t, err, "gRPC stream creation should succeed")

		err = stream.Send(&grpccompat.In{In: 1})
		require.NoError(t, err, "gRPC healthy send should succeed")
		resp, err := stream.Recv()
		require.NoError(t, err, "gRPC healthy recv should succeed")
		assert.Equal(t, int64(1), resp.Out)

		// Make server deaf to reads FROM client.
		err = env.simnet.FaultHost(env.srvName,
			rpc.DropDeafSpec{
				UpdateDeafReads:  true,
				DeafReadsNewProb: 1.0,
			},
			false,
		)
		require.NoError(t, err, "FaultHost deaf reads")

		sendErr := stream.Send(&grpccompat.In{In: 2})
		t.Logf("gRPC send during deaf: err=%v", sendErr)

		_, recvErr := stream.Recv()
		t.Logf("gRPC recv during deaf: err=%v", recvErr)

		// Repair and check recovery.
		err = env.simnet.RepairHost(env.srvName, true, true, false, true)
		require.NoError(t, err, "RepairHost")

		time.Sleep(500 * time.Millisecond)

		// New stream on same connection should work or fail cleanly.
		ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel2()

		stream2, err := env.grpcClient.Method4(ctx2)
		if err != nil {
			t.Logf("gRPC new stream after repair failed (connection torn down): %v", err)
			return
		}

		err = stream2.Send(&grpccompat.In{In: 99})
		if err == nil {
			resp, err := stream2.Recv()
			if err == nil {
				assert.Equal(t, int64(99), resp.Out,
					"gRPC new stream should work cleanly after repair")
				t.Log("gRPC new stream works cleanly after repair")
			} else {
				t.Logf("gRPC new stream recv after repair: %v", err)
			}
		} else {
			t.Logf("gRPC new stream send after repair: %v", err)
		}
	})
}
