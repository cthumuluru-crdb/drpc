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
// gRPC Baseline: ClientCloseWhileStreamsActive
// ---------------------------------------------------------------------------

func TestGRPCBaselineClientClose(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)

	runInBubble(t, func(t *testing.T) {
		goroutinesBefore := runtime.NumGoroutine()
		srv := newGRPCEchoServer()
		env := newGRPCSimnetEnv(t, seed, srv)

		const numStreams = 10

		var wg sync.WaitGroup
		streamsReady := make(chan struct{}, numStreams)
		streamErrors := make(chan error, numStreams*2)

		for i := 0; i < numStreams; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				stream, err := env.grpcClient.Method4(context.Background())
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

		for i := 0; i < numStreams; i++ {
			select {
			case <-streamsReady:
			case <-time.After(10 * time.Second):
				t.Fatalf("only %d/%d gRPC streams established", i, numStreams)
			}
		}

		t.Log("closing gRPC client conn while 10 streams are active")
		err := env.grpcConn.Close()
		assert.NoError(t, err, "grpcConn.Close should succeed")

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
			t.Log("all gRPC stream goroutines exited after client close")
		case <-time.After(10 * time.Second):
			t.Fatal("DEADLOCK: gRPC streams did not exit after client close")
		}

		close(streamErrors)
		errCount := 0
		for err := range streamErrors {
			if err != nil {
				errCount++
			}
		}
		assert.Greater(t, errCount, 0, "gRPC streams should have received errors")

		env.grpcConn = nil
		env.close()

		time.Sleep(500 * time.Millisecond)
		assertNoGoroutineLeak(t, goroutinesBefore, 15)
	})
}

// ---------------------------------------------------------------------------
// gRPC Baseline: ServerStopWhileStreamsActive
//
// gRPC equivalent of TestServerContextCancelWhileStreamsActive. Uses
// grpcSrv.Stop() (hard stop) since gRPC doesn't use a context for serving.
// ---------------------------------------------------------------------------

func TestGRPCBaselineServerStop(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)

	runInBubble(t, func(t *testing.T) {
		goroutinesBefore := runtime.NumGoroutine()
		srv := newGRPCEchoServer()
		env := newGRPCSimnetEnv(t, seed, srv)

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

				stream, err := env.grpcClient.Method4(ctx)
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

		established := 0
		for established < numStreams {
			select {
			case <-streamsReady:
				established++
			case <-time.After(10 * time.Second):
				t.Fatalf("only %d/%d gRPC streams established", established, numStreams)
			}
		}
		t.Logf("%d gRPC streams established, calling Stop()", established)

		// Hard stop — immediately closes all connections.
		env.grpcSrv.Stop()

		select {
		case <-env.serveDone:
			t.Log("gRPC server exited after Stop()")
		case <-time.After(10 * time.Second):
			t.Fatal("DEADLOCK: gRPC server did not exit after Stop()")
		}

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
			t.Log("all gRPC client streams exited after server Stop()")
		case <-time.After(10 * time.Second):
			t.Fatal("DEADLOCK: gRPC client streams did not exit after server Stop()")
		}

		close(streamErrors)
		errCount := 0
		for err := range streamErrors {
			if err != nil {
				errCount++
			}
		}
		assert.Equal(t, numStreams, errCount, "all gRPC streams should have errors after server Stop()")

		env.grpcSrv = nil // already stopped
		env.close()

		time.Sleep(500 * time.Millisecond)
		assertNoGoroutineLeak(t, goroutinesBefore, 15)
	})
}

// ---------------------------------------------------------------------------
// gRPC Baseline: TransportCloseUnderneathGRPC
//
// Close the raw net.Conn under gRPC, simulating a network-level disconnect.
// ---------------------------------------------------------------------------

func TestGRPCBaselineTransportClose(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)

	runInBubble(t, func(t *testing.T) {
		goroutinesBefore := runtime.NumGoroutine()
		srv := newGRPCEchoServer()
		env := newGRPCSimnetEnv(t, seed, srv)

		const numStreams = 5

		var wg sync.WaitGroup
		streamsReady := make(chan struct{}, numStreams)

		for i := 0; i < numStreams; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				stream, err := env.grpcClient.Method4(context.Background())
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
				t.Fatalf("only %d/%d gRPC streams established", i, numStreams)
			}
		}

		t.Log("closing raw client transport underneath gRPC")
		err := env.clientNetConn.Close()
		assert.NoError(t, err, "client transport close should succeed")

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
			t.Log("all gRPC streams exited after transport close")
		case <-time.After(10 * time.Second):
			t.Fatal("DEADLOCK: gRPC streams did not exit after transport close")
		}

		env.grpcConn = nil
		env.close()

		time.Sleep(500 * time.Millisecond)
		assertNoGoroutineLeak(t, goroutinesBefore, 15)
	})
}

// ---------------------------------------------------------------------------
// gRPC Baseline: CloseRacingWithNewStream
// ---------------------------------------------------------------------------

func TestGRPCBaselineCloseRace(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)

	runInBubble(t, func(t *testing.T) {
		goroutinesBefore := runtime.NumGoroutine()
		srv := newGRPCEchoServer()
		env := newGRPCSimnetEnv(t, seed, srv)

		const racers = 20

		var wg sync.WaitGroup
		startGun := make(chan struct{})
		var succeeded, failed atomic.Int32

		for i := 0; i < racers; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				<-startGun
				stream, err := env.grpcClient.Method4(context.Background())
				if err != nil {
					failed.Add(1)
					return
				}
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

		wg.Add(1)
		go func() {
			defer wg.Done()
			<-startGun
			time.Sleep(time.Duration(seed%5) * time.Millisecond)
			_ = env.grpcConn.Close()
		}()

		close(startGun)

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			t.Fatal("DEADLOCK: goroutines did not exit within 15s")
		}

		t.Logf("gRPC streams: succeeded=%d failed=%d", succeeded.Load(), failed.Load())
		assert.Equal(t, int32(racers), succeeded.Load()+failed.Load(),
			"all gRPC stream attempts should have resolved")

		env.grpcConn = nil
		env.close()

		time.Sleep(500 * time.Millisecond)
		assertNoGoroutineLeak(t, goroutinesBefore, 15)
	})
}

// ---------------------------------------------------------------------------
// gRPC Baseline: PerStreamContextCancel
// ---------------------------------------------------------------------------

func TestGRPCBaselinePerStreamCancel(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)

	runInBubble(t, func(t *testing.T) {
		goroutinesBefore := runtime.NumGoroutine()
		srv := newGRPCEchoServer()
		env := newGRPCSimnetEnv(t, seed, srv)
		defer env.close()

		const totalStreams = 10
		const cancelCount = 5

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
				stream, err := env.grpcClient.Method4(ctx)
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

		for i := 0; i < totalStreams; i++ {
			select {
			case <-streamsReady:
			case <-time.After(10 * time.Second):
				t.Fatalf("only %d/%d gRPC streams established", i, totalStreams)
			}
		}

		time.Sleep(500 * time.Millisecond)

		t.Logf("canceling gRPC streams 0-%d", cancelCount-1)
		for i := 0; i < cancelCount; i++ {
			cancels[i]()
		}

		time.Sleep(1 * time.Second)

		for i := cancelCount; i < totalStreams; i++ {
			cancels[i]()
		}

		outcomes := make([]streamResult, 0, totalStreams)
		for i := 0; i < totalStreams; i++ {
			select {
			case r := <-results:
				outcomes = append(outcomes, r)
			case <-time.After(10 * time.Second):
				t.Fatalf("timeout waiting for gRPC stream %d result", i)
			}
		}

		var canceledSent, survivingSent int
		for _, r := range outcomes {
			t.Logf("gRPC stream %d: canceled=%v sent=%d recv=%d err=%v",
				r.id, r.canceled, r.sent, r.received, r.err)
			if r.id < cancelCount {
				canceledSent += r.sent
			} else {
				survivingSent += r.sent
			}
		}

		t.Logf("gRPC canceled total sent: %d, surviving total sent: %d",
			canceledSent, survivingSent)
		assert.Greater(t, survivingSent, canceledSent,
			"surviving gRPC streams should have sent more than canceled ones")

		time.Sleep(500 * time.Millisecond)
		assertNoGoroutineLeak(t, goroutinesBefore, 15)
	})
}

// ---------------------------------------------------------------------------
// gRPC Baseline: ServerCloseConnectionMidStream
// ---------------------------------------------------------------------------

func TestGRPCBaselineServerConnClose(t *testing.T) {
	seed := time.Now().UnixNano()
	t.Logf("seed: %d", seed)

	runInBubble(t, func(t *testing.T) {
		goroutinesBefore := runtime.NumGoroutine()
		srv := newGRPCEchoServer()
		env := newGRPCSimnetEnv(t, seed, srv)

		const numStreams = 5

		var wg sync.WaitGroup
		streamsReady := make(chan struct{}, numStreams)

		for i := 0; i < numStreams; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				stream, err := env.grpcClient.Method4(context.Background())
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
				t.Fatalf("only %d/%d gRPC streams established", i, numStreams)
			}
		}

		t.Log("closing gRPC server-side net.Conn while streams are active")
		env.serverConnMu.Lock()
		sc := env.serverConn
		env.serverConnMu.Unlock()
		require.NotNil(t, sc, "gRPC server conn should be set")
		err := sc.Close()
		assert.NoError(t, err, "gRPC server conn close should succeed")

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
			t.Log("all gRPC client streams exited after server conn close")
		case <-time.After(10 * time.Second):
			t.Fatal("DEADLOCK: gRPC client streams did not exit after server conn close")
		}

		env.grpcConn = nil
		env.serverConn = nil
		env.close()

		time.Sleep(500 * time.Millisecond)
		assertNoGoroutineLeak(t, goroutinesBefore, 15)
	})
}
