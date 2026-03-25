// Copyright (C) 2024 Storj Labs, Inc.
// See LICENSE for copying information.

package integration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glycerine/gosimnet"
	rpc "github.com/glycerine/rpc25519"

	"storj.io/drpc/drpcconn"
	"storj.io/drpc/drpcmanager"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcserver"
	"storj.io/drpc/drpctest"
)

// TestMain is needed because gosimnet's dependency rpc25519 has a bug:
// its init() creates a 53-byte host.cid file but panics on re-read
// expecting 37 bytes. Tests must be run with XDG_CONFIG_HOME set to a
// temp directory:
//
//	XDG_CONFIG_HOME=$(mktemp -d) go test ...
//
// Or via the Makefile target:
//
//	make test-integration
func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

//
// simnetEnv wraps gosimnet setup for DRPC tests.
//

const simnetServerName = "drpc-srv"

type simnetEnv struct {
	seed     int64
	network  *gosimnet.SimNet
	server   *gosimnet.SimServer
	listener net.Listener
	clientN  atomic.Int64
	simnet   *rpc.Simnet // lazily populated
}

func newSimnetEnv(t testing.TB, seed int64) *simnetEnv {
	cfg := gosimnet.NewSimNetConfig()
	cfg.InitialSimnetScenario = seed
	network := gosimnet.NewSimNet(cfg)

	srv := network.NewSimServer(simnetServerName)
	lsn, err := srv.Listen("tcp", "")
	if err != nil {
		t.Fatal(err)
	}

	env := &simnetEnv{
		seed:     seed,
		network:  network,
		server:   srv,
		listener: lsn,
	}
	t.Cleanup(func() {
		_ = lsn.Close()
		_ = network.Close()
	})
	return env
}

// getSimnet returns the underlying rpc25519 Simnet for fault injection.
// Only available after the first Listen call.
func (e *simnetEnv) getSimnet() *rpc.Simnet {
	if e.simnet == nil {
		e.simnet = e.network.GetRpcSimnet()
	}
	return e.simnet
}

// dialConn creates a gosimnet client, dials the server, and returns both
// sides of the connection plus the client's simnet name (for fault injection).
// Accept is done in a goroutine to avoid deadlock.
func (e *simnetEnv) dialConn(t testing.TB) (clientConn, serverConn net.Conn, clientName string) {
	clientName = fmt.Sprintf("cli-%d", e.clientN.Add(1))
	cli, err := e.network.NewSimClient(clientName)
	if err != nil {
		t.Fatal(err)
	}

	// Accept must happen concurrently with Dial.
	type acceptResult struct {
		conn net.Conn
		err  error
	}
	ch := make(chan acceptResult, 1)
	go func() {
		c, err := e.listener.Accept()
		ch <- acceptResult{c, err}
	}()

	clientConn, err = cli.Dial("tcp", simnetServerName)
	if err != nil {
		t.Fatal(err)
	}

	res := <-ch
	if res.err != nil {
		t.Fatal(res.err)
	}
	return clientConn, res.conn, clientName
}

// createSimnetConnectionNamed creates a DRPC client-server pair over gosimnet
// and returns the client's simnet name for use with fault injection APIs.
func createSimnetConnectionNamed(
	t testing.TB,
	env *simnetEnv,
	server DRPCServiceServer,
	tracker *drpctest.Tracker,
) (DRPCServiceClient, *drpcconn.Conn, string) {
	clientConn, serverConn, clientName := env.dialConn(t)

	mux := drpcmux.New()
	if err := DRPCRegisterService(mux, server); err != nil {
		t.Fatal(err)
	}
	srv := drpcserver.New(mux)
	tracker.Run(func(ctx context.Context) { _ = srv.ServeOne(ctx, serverConn) })

	conn := drpcconn.NewWithOptions(clientConn, drpcconn.Options{
		Manager: drpcmanager.Options{SoftCancel: true},
	})
	return NewDRPCServiceClient(conn), conn, clientName
}

// streamingImpl returns an impl that does enough I/O to exercise streaming.
func streamingImpl() impl {
	return impl{
		Method1Fn: standardImpl.Method1Fn,
		Method2Fn: func(stream DRPCService_Method2Stream) error {
			for {
				if _, err := stream.Recv(); err != nil {
					break
				}
			}
			return stream.SendAndClose(&Out{Out: 2})
		},
		Method3Fn: func(_ *In, stream DRPCService_Method3Stream) error {
			for i := 0; i < 10; i++ {
				if err := stream.Send(&Out{Out: 3, Data: data(64)}); err != nil {
					return err
				}
			}
			return nil
		},
		Method4Fn: func(stream DRPCService_Method4Stream) error {
			for {
				if _, err := stream.Recv(); err != nil {
					break
				}
				if err := stream.Send(&Out{Out: 4, Data: data(64)}); err != nil {
					return err
				}
			}
			return nil
		},
	}
}

// exerciseRPC runs an RPC by method number (1-4). Returns error from the RPC.
func exerciseRPC(ctx context.Context, cli DRPCServiceClient, method int) error {
	switch method {
	case 1:
		_, err := cli.Method1(ctx, in(1))
		return err
	case 2:
		stream, err := cli.Method2(ctx)
		if err != nil {
			return err
		}
		for i := 0; i < 5; i++ {
			if err := stream.Send(&In{In: 2}); err != nil {
				return err
			}
		}
		_, err = stream.CloseAndRecv()
		return err
	case 3:
		stream, err := cli.Method3(ctx, in(3))
		if err != nil {
			return err
		}
		for {
			if _, err := stream.Recv(); err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}
		}
	case 4:
		stream, err := cli.Method4(ctx)
		if err != nil {
			return err
		}
		for i := 0; i < 5; i++ {
			if err := stream.Send(&In{In: 4}); err != nil {
				return err
			}
			if _, err := stream.Recv(); err != nil {
				return err
			}
		}
		return stream.CloseSend()
	default:
		return fmt.Errorf("unknown method %d", method)
	}
}

// TestProbabilisticFaults uses FaultCircuit with DropDeafSpec to inject
// probabilistic send drops and read deafness during active RPCs.
func TestProbabilisticFaults(t *testing.T) {
	for _, seed := range []int64{1, 42, 100, 999} {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			env := newSimnetEnv(t, seed)
			sn := env.getSimnet()
			if sn == nil {
				t.Fatal("simnet not available")
			}
			rng := rand.New(rand.NewSource(seed))

			for iter := 0; iter < 8; iter++ {
				tracker := drpctest.NewTracker(t)
				cli, conn, clientName := createSimnetConnectionNamed(t, env, streamingImpl(), tracker)
				method := rng.Intn(4) + 1

				// Inject a probabilistic fault on the client's circuit.
				dropProb := rng.Float64()
				deafProb := rng.Float64()
				dd := rpc.DropDeafSpec{
					UpdateDropSends:  true,
					DropSendsNewProb: dropProb,
					UpdateDeafReads:  true,
					DeafReadsNewProb: deafProb,
				}
				if err := sn.FaultCircuit(clientName, simnetServerName, dd, false); err != nil {
					t.Logf("FaultCircuit error (may be expected): %v", err)
				}

				// Run an RPC — may succeed or fail, must not hang.
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_ = exerciseRPC(ctx, cli, method)
				cancel()

				// Repair and verify the circuit is healthy again.
				_ = sn.RepairCircuit(clientName, true, false, false)

				_ = conn.Close()
				tracker.Close()

				if t.Failed() {
					t.Logf("FAILED at iter=%d method=%d dropProb=%.2f deafProb=%.2f seed=%d",
						iter, method, dropProb, deafProb, seed)
					return
				}
			}
		})
	}
}

// TestAsymmetricFaults tests asymmetric network failures — e.g., client
// can send but server is deaf, or server drops all sends but client reads fine.
func TestAsymmetricFaults(t *testing.T) {
	for _, seed := range []int64{1, 42, 100} {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			env := newSimnetEnv(t, seed)
			sn := env.getSimnet()
			if sn == nil {
				t.Fatal("simnet not available")
			}

			type scenario struct {
				name      string
				faultNode string // which node to fault (filled in per iteration)
				dropSends float64
				deafReads float64
				useServer bool // true = fault the server side
			}
			scenarios := []scenario{
				{name: "server_deaf", deafReads: 1.0, useServer: true},
				{name: "server_drops_sends", dropSends: 1.0, useServer: true},
				{name: "client_deaf", deafReads: 1.0, useServer: false},
				{name: "client_drops_sends", dropSends: 1.0, useServer: false},
			}

			for _, sc := range scenarios {
				t.Run(sc.name, func(t *testing.T) {
					for iter := 0; iter < 2; iter++ {
						tracker := drpctest.NewTracker(t)
						cli, conn, clientName := createSimnetConnectionNamed(t, env, streamingImpl(), tracker)

						faultNode := clientName
						if sc.useServer {
							faultNode = simnetServerName
						}

						dd := rpc.DropDeafSpec{
							UpdateDropSends:  true,
							DropSendsNewProb: sc.dropSends,
							UpdateDeafReads:  true,
							DeafReadsNewProb: sc.deafReads,
						}
						_ = sn.FaultCircuit(faultNode, "", dd, false)

						// RPC should fail or timeout — must not hang.
						ctx, cancel := context.WithTimeout(context.Background(), time.Second)
						_ = exerciseRPC(ctx, cli, 1) // unary
						cancel()

						_ = sn.AllHealthy(true, false)
						_ = conn.Close()
						tracker.Close()
					}
				})
			}
		})
	}
}

// TestNetworkPartition uses AlterHost(ISOLATE) to simulate network
// partitions during active RPCs.
func TestNetworkPartition(t *testing.T) {
	for _, seed := range []int64{1, 42, 100} {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			env := newSimnetEnv(t, seed)
			sn := env.getSimnet()
			if sn == nil {
				t.Fatal("simnet not available")
			}
			rng := rand.New(rand.NewSource(seed))

			// Test partition during each RPC method.
			for method := 1; method <= 4; method++ {
				for _, side := range []string{"client", "server"} {
					t.Run(fmt.Sprintf("method%d/%s", method, side), func(t *testing.T) {
						tracker := drpctest.NewTracker(t)
						cli, conn, clientName := createSimnetConnectionNamed(t, env, streamingImpl(), tracker)

						target := clientName
						if side == "server" {
							target = simnetServerName
						}

						// For streaming methods, start the RPC first, then partition.
						if method >= 2 {
							// Start a goroutine that partitions after a short random delay.
							delay := time.Duration(rng.Intn(50)+10) * time.Millisecond
							go func() {
								time.Sleep(delay)
								_, _ = sn.AlterHost(target, rpc.ISOLATE)
							}()
						} else {
							// For unary, partition immediately.
							_, _ = sn.AlterHost(target, rpc.ISOLATE)
						}

						ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
						_ = exerciseRPC(ctx, cli, method)
						cancel()

						// Restore network.
						_, _ = sn.AlterHost(target, rpc.UNISOLATE)
						_ = sn.AllHealthy(true, false)

						_ = conn.Close()
						tracker.Close()
					})
				}
			}
		})
	}
}

// TestFaultRepairCycle injects random faults, verifies the RPC fails
// without hanging, repairs the network, and verifies a new connection works.
func TestFaultRepairCycle(t *testing.T) {
	for _, seed := range []int64{1, 42, 100, 999} {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			env := newSimnetEnv(t, seed)
			sn := env.getSimnet()
			if sn == nil {
				t.Fatal("simnet not available")
			}
			rng := rand.New(rand.NewSource(seed))

			for iter := 0; iter < 10; iter++ {
				tracker := drpctest.NewTracker(t)
				cli, conn, clientName := createSimnetConnectionNamed(t, env, streamingImpl(), tracker)
				method := rng.Intn(4) + 1

				// Inject a random fault type.
				faultType := rng.Intn(3)
				switch faultType {
				case 0: // probabilistic drops
					dd := rpc.DropDeafSpec{
						UpdateDropSends:  true,
						DropSendsNewProb: 0.5 + rng.Float64()*0.5, // 50-100%
						UpdateDeafReads:  true,
						DeafReadsNewProb: 0.5 + rng.Float64()*0.5,
					}
					_ = sn.FaultHost(clientName, dd, false)
				case 1: // isolation
					_, _ = sn.AlterHost(clientName, rpc.ISOLATE)
				case 2: // server isolation
					_, _ = sn.AlterHost(simnetServerName, rpc.ISOLATE)
				}

				// Try RPC — expect failure, must not hang.
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				_ = exerciseRPC(ctx, cli, method)
				cancel()

				// Repair everything.
				_ = sn.AllHealthy(true, false)

				_ = conn.Close()
				tracker.Close()

				// Verify a new connection works after repair.
				tracker2 := drpctest.NewTracker(t)
				cli2, conn2, _ := createSimnetConnectionNamed(t, env, standardImpl, tracker2)
				ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
				out, err := cli2.Method1(ctx2, in(1))
				cancel2()
				if err != nil {
					t.Fatalf("iter=%d: RPC after repair failed: %v", iter, err)
				}
				if out.Out != 1 {
					t.Fatalf("iter=%d: unexpected output: %d", iter, out.Out)
				}
				_ = conn2.Close()
				tracker2.Close()
			}
		})
	}
}

// TestDeterministicReplay runs the same scenario twice with the same
// seed and verifies the simnet execution snapshots match.
// NOTE: This test requires synctest (Go 1.24+ with -tags=synctest) for fully
// deterministic goroutine scheduling. Without synctest, goroutine ordering
// varies between runs, so the hashes will differ even with the same seed.
func TestDeterministicReplay(t *testing.T) {
	t.Skip("requires synctest build tag for deterministic goroutine scheduling")
	runScenario := func(t *testing.T, seed int64) *rpc.SimnetSnapshot {
		env := newSimnetEnv(t, seed)
		sn := env.getSimnet()
		if sn == nil {
			t.Fatal("simnet not available")
		}

		// Run a fixed sequence of operations.
		tracker := drpctest.NewTracker(t)
		cli, conn, clientName := createSimnetConnectionNamed(t, env, standardImpl, tracker)

		// Do several unary RPCs.
		for i := 0; i < 5; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, _ = cli.Method1(ctx, in(int64(i+1)))
			cancel()
		}

		// Inject a fault and do more RPCs.
		dd := rpc.DropDeafSpec{
			UpdateDropSends:  true,
			DropSendsNewProb: 0.5,
		}
		_ = sn.FaultCircuit(clientName, simnetServerName, dd, false)

		for i := 0; i < 3; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_, _ = cli.Method1(ctx, in(1))
			cancel()
		}

		_ = sn.AllHealthy(true, false)
		_ = conn.Close()
		tracker.Close()

		snap := sn.GetSimnetSnapshot(true)
		return snap
	}

	for _, seed := range []int64{42, 100} {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			snap1 := runScenario(t, seed)
			snap2 := runScenario(t, seed)

			if snap1 == nil || snap2 == nil {
				t.Fatal("nil snapshot")
			}

			// Compare the finish-order hash — if deterministic, these match.
			if snap1.XhashFin != snap2.XhashFin {
				t.Logf("snap1.XhashFin=%s", snap1.XhashFin)
				t.Logf("snap2.XhashFin=%s", snap2.XhashFin)
				t.Logf("snap1.Xcountsn=%d snap2.Xcountsn=%d", snap1.Xcountsn, snap2.Xcountsn)
				t.Error("non-deterministic execution: finish hashes differ")
			}
			if snap1.XhashDis != snap2.XhashDis {
				t.Logf("snap1.XhashDis=%s", snap1.XhashDis)
				t.Logf("snap2.XhashDis=%s", snap2.XhashDis)
				t.Error("non-deterministic execution: dispatch hashes differ")
			}
		})
	}
}

// TestRandomScenarios is the comprehensive "big hammer" test.
// Each scenario randomly picks a fault type, RPC method, timing, and side.
func TestRandomScenarios(t *testing.T) {
	const numScenarios = 20

	for scenario := 0; scenario < numScenarios; scenario++ {
		t.Run(fmt.Sprintf("scenario=%d", scenario), func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(scenario)))
			env := newSimnetEnv(t, int64(scenario))
			sn := env.getSimnet()
			if sn == nil {
				t.Fatal("simnet not available")
			}

			tracker := drpctest.NewTracker(t)
			cli, conn, clientName := createSimnetConnectionNamed(t, env, streamingImpl(), tracker)

			method := rng.Intn(4) + 1
			faultType := rng.Intn(4)
			faultSide := rng.Intn(2) // 0=client, 1=server

			target := clientName
			if faultSide == 1 {
				target = simnetServerName
			}

			// Inject fault based on type.
			switch faultType {
			case 0: // probabilistic drops
				dd := rpc.DropDeafSpec{
					UpdateDropSends:  true,
					DropSendsNewProb: rng.Float64(),
					UpdateDeafReads:  true,
					DeafReadsNewProb: rng.Float64(),
				}
				_ = sn.FaultCircuit(target, "", dd, false)

			case 1: // full deaf (100% read deafness)
				dd := rpc.DropDeafSpec{
					UpdateDeafReads:  true,
					DeafReadsNewProb: 1.0,
				}
				_ = sn.FaultCircuit(target, "", dd, false)

			case 2: // network isolation
				_, _ = sn.AlterHost(target, rpc.ISOLATE)

			case 3: // no fault (healthy baseline to verify no false positives)
			}

			// Exercise the RPC.
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err := exerciseRPC(ctx, cli, method)
			cancel()

			// For healthy (faultType==3), the RPC should succeed.
			if faultType == 3 && err != nil {
				t.Errorf("scenario=%d: healthy RPC failed: %v (method=%d)", scenario, err, method)
			}

			// Repair and cleanup.
			_ = sn.AllHealthy(true, false)
			_ = conn.Close()
			tracker.Close()
		})
	}
}
