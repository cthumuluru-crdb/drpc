package simnet

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"storj.io/drpc/internal/grpccompat"
)

// ---------------------------------------------------------------------------
// Test: ClientCloseWhileStreamsActive
//
// Client calls drpcConn.Close() while 10 bidi streams are mid-exchange.
// Verifies:
//   - All stream Recv()/Send() return errors (not hang)
//   - Server's ServeOne exits cleanly
//   - No goroutine leaks
// ---------------------------------------------------------------------------

func TestClientCloseWhileStreamsActive(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)

	runInBubble(t, func(t *testing.T) {
		goroutinesBefore := runtime.NumGoroutine()
		srv := newEchoServer()
		env := newSimnetEnv(t, seed, srv)

		const numStreams = 10

		// Open streams and start exchanging messages.
		var wg sync.WaitGroup
		streamsReady := make(chan struct{}, numStreams)
		streamErrors := make(chan error, numStreams*2)

		for i := 0; i < numStreams; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				stream, err := env.drpcClient.Method4(context.Background())
				if err != nil {
					streamErrors <- err
					return
				}
				streamsReady <- struct{}{}

				for seq := 0; ; seq++ {
					if err := stream.Send(&grpccompat.In{In: int64(seq)}); err != nil {
						streamErrors <- err
						return
					}
					if _, err := stream.Recv(); err != nil {
						streamErrors <- err
						return
					}
				}
			}(i)
		}

		// Wait for all streams to be established.
		for i := 0; i < numStreams; i++ {
			select {
			case <-streamsReady:
			case <-time.After(10 * time.Second):
				t.Fatalf("only %d/%d streams established", i, numStreams)
			}
		}

		// ACT: Close the client connection while streams are active.
		t.Log("closing client drpcConn while 10 streams are active")
		err := env.drpcConn.Close()
		assert.NoError(t, err, "drpcConn.Close should succeed")

		// All stream goroutines must exit (not hang).
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
			t.Log("all stream goroutines exited after client close")
		case <-time.After(10 * time.Second):
			t.Fatal("DEADLOCK: streams did not exit within 10s after client close")
		}

		// Server's ServeOne should also exit.
		select {
		case <-env.serveDone:
			t.Log("server ServeOne exited cleanly")
		case <-time.After(10 * time.Second):
			t.Fatal("DEADLOCK: server ServeOne did not exit after client close")
		}

		// Collect stream errors — every stream should have gotten one.
		close(streamErrors)
		errCount := 0
		for err := range streamErrors {
			if err != nil {
				errCount++
			}
		}
		assert.Greater(t, errCount, 0, "streams should have received errors")

		// Cleanup (drpcConn already closed, skip that in close).
		env.drpcConn = nil
		env.close()

		time.Sleep(200 * time.Millisecond)
		assertNoGoroutineLeak(t, goroutinesBefore, 5)
	})
}

// ---------------------------------------------------------------------------
// Test: ServerContextCancelWhileStreamsActive
//
// Cancel the srvCtx (passed to ServeOne) while streams are active. This is
// the server-initiated graceful shutdown path.
// Verifies:
//   - Server handler goroutines exit
//   - Client streams get errors on next Recv()
//   - Connection closes cleanly
// ---------------------------------------------------------------------------

func TestServerContextCancelWhileStreamsActive(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)

	runInBubble(t, func(t *testing.T) {
		goroutinesBefore := runtime.NumGoroutine()
		srv := newEchoServer()
		env := newSimnetEnv(t, seed, srv)
		defer env.close()

		const numStreams = 10

		var wg sync.WaitGroup
		streamsReady := make(chan struct{}, numStreams)
		streamErrors := make(chan error, numStreams)

		for i := 0; i < numStreams; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()

				stream, err := env.drpcClient.Method4(ctx)
				if err != nil {
					streamErrors <- err
					return
				}
				streamsReady <- struct{}{}

				for seq := 0; ; seq++ {
					if err := stream.Send(&grpccompat.In{In: int64(seq)}); err != nil {
						streamErrors <- err
						return
					}
					if _, err := stream.Recv(); err != nil {
						streamErrors <- err
						return
					}
				}
			}(i)
		}

		// Wait for streams to establish.
		established := 0
		for established < numStreams {
			select {
			case <-streamsReady:
				established++
			case <-time.After(10 * time.Second):
				t.Fatalf("only %d/%d streams established", established, numStreams)
			}
		}
		t.Logf("%d streams established, canceling server context", established)

		// ACT: Cancel the server context.
		env.srvCancel()

		// Server's ServeOne should exit.
		select {
		case <-env.serveDone:
			t.Log("server ServeOne exited after context cancel")
		case <-time.After(10 * time.Second):
			t.Fatal("DEADLOCK: server ServeOne did not exit after context cancel")
		}

		// All client streams should get errors (not hang).
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
			t.Log("all client streams exited after server context cancel")
		case <-time.After(10 * time.Second):
			t.Fatal("DEADLOCK: client streams did not exit after server context cancel")
		}

		close(streamErrors)
		errCount := 0
		for err := range streamErrors {
			if err != nil {
				errCount++
			}
		}
		assert.Equal(t, numStreams, errCount, "all streams should have errors after server cancel")

		time.Sleep(200 * time.Millisecond)
		assertNoGoroutineLeak(t, goroutinesBefore, 5)
	})
}

// ---------------------------------------------------------------------------
// Test: TransportCloseUnderneathDRPC
//
// Close the raw net.Conn (simnet transport) directly, bypassing DRPC's
// Close(). This simulates a network-level disconnect.
// Verifies:
//   - Manager's manageReader detects the read error
//   - All active streams get terminated
//   - No panic, no goroutine leak
// ---------------------------------------------------------------------------

func TestTransportCloseUnderneathDRPC(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)

	runInBubble(t, func(t *testing.T) {
		goroutinesBefore := runtime.NumGoroutine()
		srv := newEchoServer()
		env := newSimnetEnv(t, seed, srv)

		const numStreams = 5

		var wg sync.WaitGroup
		streamsReady := make(chan struct{}, numStreams)

		for i := 0; i < numStreams; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				stream, err := env.drpcClient.Method4(context.Background())
				if err != nil {
					return
				}
				streamsReady <- struct{}{}

				for seq := 0; ; seq++ {
					if err := stream.Send(&grpccompat.In{In: int64(seq)}); err != nil {
						return
					}
					if _, err := stream.Recv(); err != nil {
						return
					}
				}
			}(i)
		}

		for i := 0; i < numStreams; i++ {
			select {
			case <-streamsReady:
			case <-time.After(10 * time.Second):
				t.Fatalf("only %d/%d streams established", i, numStreams)
			}
		}

		// ACT: Close the underlying transport directly, bypassing DRPC.
		t.Log("closing raw transport underneath DRPC")
		tr := env.drpcConn.Transport()
		err := tr.Close()
		assert.NoError(t, err, "transport close should succeed")

		// All stream goroutines should exit.
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
			t.Log("all streams exited after transport close")
		case <-time.After(10 * time.Second):
			t.Fatal("DEADLOCK: streams did not exit after transport close")
		}

		// The DRPC conn should be in a closed state.
		select {
		case <-env.drpcConn.Closed():
			t.Log("drpcConn reports closed after transport close")
		default:
			t.Error("drpcConn.Closed() channel not closed after transport close")
		}

		// Server should also detect the broken transport.
		select {
		case <-env.serveDone:
			t.Log("server ServeOne exited after transport close")
		case <-time.After(10 * time.Second):
			t.Fatal("DEADLOCK: server did not exit after transport close")
		}

		env.drpcConn = nil // already broken, skip in close
		env.close()

		time.Sleep(200 * time.Millisecond)
		assertNoGoroutineLeak(t, goroutinesBefore, 5)
	})
}

// ---------------------------------------------------------------------------
// Test: CloseRacingWithNewStream
//
// Race drpcConn.Close() against NewClientStream() from multiple goroutines.
// Tests the TOCTOU between activeStreams.Add() and sigs.term.
// Verifies:
//   - NewClientStream returns error (not panic) when connection is closing
//   - No streams leak (stuck in activeStreams after close)
//   - No goroutine leaks
// ---------------------------------------------------------------------------

func TestCloseRacingWithNewStream(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)

	runInBubble(t, func(t *testing.T) {
		goroutinesBefore := runtime.NumGoroutine()
		srv := newEchoServer()
		env := newSimnetEnv(t, seed, srv)

		const racers = 20

		// ACT: Simultaneously close the connection and try to open streams.
		var wg sync.WaitGroup
		startGun := make(chan struct{})
		var succeeded, failed atomic.Int32

		// Half the goroutines try to open streams.
		for i := 0; i < racers; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				<-startGun
				stream, err := env.drpcClient.Method4(context.Background())
				if err != nil {
					failed.Add(1)
					return
				}
				// Try to use the stream briefly.
				if err := stream.Send(&grpccompat.In{In: int64(id)}); err != nil {
					failed.Add(1)
					return
				}
				if _, err := stream.Recv(); err != nil {
					failed.Add(1)
					return
				}
				succeeded.Add(1)
			}(i)
		}

		// One goroutine closes the connection.
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startGun
			// Small random delay so some streams may have started.
			time.Sleep(time.Duration(seed%5) * time.Millisecond)
			_ = env.drpcConn.Close()
		}()

		// Fire!
		close(startGun)

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
			// No deadlock.
		case <-time.After(15 * time.Second):
			t.Fatal("DEADLOCK: goroutines did not exit within 15s")
		}

		t.Logf("streams: succeeded=%d failed=%d", succeeded.Load(), failed.Load())

		// The key assertion: no panic reached here, and all goroutines exited.
		// Some streams may succeed (opened before close), some may fail.
		assert.Equal(t, int32(racers), succeeded.Load()+failed.Load(),
			"all stream attempts should have resolved (no stuck goroutines)")

		env.drpcConn = nil
		env.close()

		time.Sleep(200 * time.Millisecond)
		assertNoGoroutineLeak(t, goroutinesBefore, 5)
	})
}

// ---------------------------------------------------------------------------
// Test: PerStreamContextCancel
//
// 10 streams, cancel 5 of them individually while the other 5 continue.
// Verifies:
//   - Canceled streams exit with context errors
//   - Surviving streams keep working (no cross-stream contamination)
//   - This is the core multiplexing independence property under cancellation
// ---------------------------------------------------------------------------

func TestPerStreamContextCancel(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)

	runInBubble(t, func(t *testing.T) {
		goroutinesBefore := runtime.NumGoroutine()
		srv := newEchoServer()
		env := newSimnetEnv(t, seed, srv)
		defer env.close()

		const totalStreams = 10
		const cancelCount = 5
		const msgsAfterCancel = 20

		type streamResult struct {
			id       int
			canceled bool
			sent     int
			received int
			err      error
		}

		results := make(chan streamResult, totalStreams)
		cancels := make([]context.CancelFunc, totalStreams)
		streamsReady := make(chan int, totalStreams)

		for i := 0; i < totalStreams; i++ {
			ctx, cancel := context.WithCancel(context.Background())
			cancels[i] = cancel

			go func(id int) {
				defer cancel()
				stream, err := env.drpcClient.Method4(ctx)
				if err != nil {
					results <- streamResult{id: id, err: err}
					return
				}
				streamsReady <- id

				var sent, received int
				for seq := 0; ; seq++ {
					if err := stream.Send(&grpccompat.In{In: int64(seq)}); err != nil {
						results <- streamResult{id: id, canceled: id < cancelCount, sent: sent, received: received, err: err}
						return
					}
					sent++
					if _, err := stream.Recv(); err != nil {
						results <- streamResult{id: id, canceled: id < cancelCount, sent: sent, received: received, err: err}
						return
					}
					received++
				}
			}(i)
		}

		// Wait for all streams to establish.
		for i := 0; i < totalStreams; i++ {
			select {
			case <-streamsReady:
			case <-time.After(10 * time.Second):
				t.Fatalf("only %d/%d streams established", i, totalStreams)
			}
		}

		// Let them exchange some messages.
		time.Sleep(500 * time.Millisecond)

		// ACT: Cancel the first 5 streams.
		t.Logf("canceling streams 0-%d", cancelCount-1)
		for i := 0; i < cancelCount; i++ {
			cancels[i]()
		}

		// Let the surviving streams continue.
		time.Sleep(1 * time.Second)

		// Cancel the remaining streams to collect results.
		for i := cancelCount; i < totalStreams; i++ {
			cancels[i]()
		}

		// Collect all results.
		outcomes := make([]streamResult, 0, totalStreams)
		for i := 0; i < totalStreams; i++ {
			select {
			case r := <-results:
				outcomes = append(outcomes, r)
			case <-time.After(10 * time.Second):
				t.Fatalf("timeout waiting for stream %d result", i)
			}
		}

		// ASSERT
		var canceledSent, survivingSent int
		for _, r := range outcomes {
			t.Logf("stream %d: canceled=%v sent=%d recv=%d err=%v",
				r.id, r.canceled, r.sent, r.received, r.err)
			if r.id < cancelCount {
				canceledSent += r.sent
			} else {
				survivingSent += r.sent
			}
		}

		// Surviving streams should have sent more messages than canceled ones
		// because they ran longer.
		t.Logf("canceled streams total sent: %d, surviving streams total sent: %d",
			canceledSent, survivingSent)
		assert.Greater(t, survivingSent, canceledSent,
			"surviving streams should have sent more than canceled ones")

		time.Sleep(200 * time.Millisecond)
		assertNoGoroutineLeak(t, goroutinesBefore, 5)
	})
}

// ---------------------------------------------------------------------------
// Test: ServerCloseConnectionMidStream
//
// Server handler explicitly closes its net.Conn while client streams are
// active. Different from TransportCloseUnderneathDRPC because the server
// initiates the close.
// Verifies:
//   - Client gets errors (not hangs)
//   - Server's ServeOne exits cleanly
//   - No goroutine leaks
// ---------------------------------------------------------------------------

func TestServerCloseConnectionMidStream(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)

	runInBubble(t, func(t *testing.T) {
		goroutinesBefore := runtime.NumGoroutine()
		srv := newEchoServer()
		env := newSimnetEnv(t, seed, srv)

		const numStreams = 5

		var wg sync.WaitGroup
		streamsReady := make(chan struct{}, numStreams)

		for i := 0; i < numStreams; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				stream, err := env.drpcClient.Method4(context.Background())
				if err != nil {
					return
				}
				streamsReady <- struct{}{}

				for seq := 0; ; seq++ {
					if err := stream.Send(&grpccompat.In{In: int64(seq)}); err != nil {
						return
					}
					if _, err := stream.Recv(); err != nil {
						return
					}
				}
			}(i)
		}

		for i := 0; i < numStreams; i++ {
			select {
			case <-streamsReady:
			case <-time.After(10 * time.Second):
				t.Fatalf("only %d/%d streams established", i, numStreams)
			}
		}

		// ACT: Close the server-side net.Conn directly.
		t.Log("closing server-side net.Conn while streams are active")
		env.serverConnMu.Lock()
		sc := env.serverConn
		env.serverConnMu.Unlock()
		require.NotNil(t, sc, "server conn should be set")
		err := sc.Close()
		assert.NoError(t, err, "server conn close should succeed")

		// All client streams should exit.
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
			t.Log("all client streams exited after server conn close")
		case <-time.After(10 * time.Second):
			t.Fatal("DEADLOCK: client streams did not exit after server conn close")
		}

		// Server's ServeOne should exit.
		select {
		case <-env.serveDone:
			t.Log("server ServeOne exited after conn close")
		case <-time.After(10 * time.Second):
			t.Fatal("DEADLOCK: server ServeOne did not exit after conn close")
		}

		env.drpcConn = nil
		env.serverConn = nil
		env.close()

		time.Sleep(200 * time.Millisecond)
		assertNoGoroutineLeak(t, goroutinesBefore, 5)
	})
}
