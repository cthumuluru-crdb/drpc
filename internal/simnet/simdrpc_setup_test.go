package simnet

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	rpc "github.com/glycerine/rpc25519"
	"storj.io/drpc/drpcconn"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcserver"
	"storj.io/drpc/internal/grpccompat"
)

// simnetEnv encapsulates a gosimnet-backed DRPC server and client.
// The server accepts a single connection; the client dials it.
// All network I/O flows through rpc25519's deterministic simnet scheduler.
type simnetEnv struct {
	t       testing.TB
	seed    int64
	srvName string // simnet node name, for use in FaultCircuit/AlterHost
	cliName string
	cfg     *rpc.Config
	srv     *rpc.Server
	cli     *rpc.Client
	simnet  *rpc.Simnet

	// DRPC layer
	drpcSrv    *drpcserver.Server
	drpcConn   *drpcconn.Conn
	drpcClient grpccompat.DRPCServiceClient

	// server-side net.Conn tracking for cleanup
	serverConn   net.Conn
	serverConnMu sync.Mutex

	// context for server-side ServeOne
	srvCtx    context.Context
	srvCancel context.CancelFunc

	// tracks when ServeOne exits
	serveDone chan struct{}
}

// newSimnetEnv creates a gosimnet environment and wires DRPC on top of it.
// The server implements the handlers provided by serverImpl.
// Call env.close() when done.
// envCounter ensures each simnetEnv gets unique server/client names,
// preventing collisions when the same test creates multiple environments.
var envCounter atomic.Int64

func newSimnetEnv(t testing.TB, seed int64, serverImpl grpccompat.DRPCServiceServer) *simnetEnv {
	t.Helper()

	id := envCounter.Add(1)

	cfg := rpc.NewConfig()
	cfg.UseSimNet = true
	cfg.InitialSimnetScenario = seed

	cfg.ServerAddr = "127.0.0.1:0"

	srvName := fmt.Sprintf("srv_%s_%d", t.Name(), id)
	srv := rpc.NewServer(srvName, cfg)

	lsn, err := srv.Listen("simnet", srvName)
	if err != nil {
		t.Fatalf("srv.Listen: %v", err)
	}

	simnet := cfg.GetSimnet()
	if simnet == nil {
		t.Fatal("simnet is nil after Listen")
	}

	serverAddr := lsn.Addr()

	// Set up DRPC server handler
	mux := drpcmux.New()
	if err := grpccompat.DRPCRegisterService(mux, serverImpl); err != nil {
		t.Fatalf("DRPCRegisterService: %v", err)
	}
	drpcSrv := drpcserver.New(mux)

	srvCtx, srvCancel := context.WithCancel(context.Background())

	cliName := fmt.Sprintf("cli_%s_%d", t.Name(), id)

	env := &simnetEnv{
		t:         t,
		seed:      seed,
		srvName:   srvName,
		cliName:   cliName,
		cfg:       cfg,
		srv:       srv,
		simnet:    simnet,
		drpcSrv:   drpcSrv,
		srvCtx:    srvCtx,
		srvCancel: srvCancel,
		serveDone: make(chan struct{}),
	}

	// Accept a server-side connection and serve DRPC on it.
	go func() {
		defer close(env.serveDone)
		sc, err := lsn.Accept()
		if err != nil {
			return
		}
		env.serverConnMu.Lock()
		env.serverConn = sc
		env.serverConnMu.Unlock()
		_ = drpcSrv.ServeOne(srvCtx, sc)
	}()

	// Dial the client side
	cfg.ClientDialToHostPort = serverAddr.String()
	client, err2 := rpc.NewClient(cliName, cfg)
	if err2 != nil {
		t.Fatalf("NewClient: %v", err2)
	}
	env.cli = client

	clientConn, err2 := client.Dial("simnet", serverAddr.String())
	if err2 != nil {
		t.Fatalf("cli.Dial: %v", err2)
	}

	env.drpcConn = drpcconn.New(clientConn)
	env.drpcClient = grpccompat.NewDRPCServiceClient(env.drpcConn)

	return env
}

// close tears down the environment in order: DRPC conn, rpc25519 server/client, simnet.
func (e *simnetEnv) close() {
	if e.drpcConn != nil {
		_ = e.drpcConn.Close()
	}
	e.srvCancel()
	<-e.serveDone

	e.serverConnMu.Lock()
	if e.serverConn != nil {
		_ = e.serverConn.Close()
	}
	e.serverConnMu.Unlock()

	if e.cli != nil {
		e.cli.Close()
	}
	if e.srv != nil {
		e.srv.Close()
	}
	if e.simnet != nil {
		e.simnet.Close()
	}
}

// echoServer is a simple DRPCServiceServer that echoes back messages.
type echoServer struct {
	grpccompat.DRPCServiceUnimplementedServer

	// mu protects activeStreams counter
	mu            sync.Mutex
	activeStreams int
	streamsClosed chan struct{} // closed when activeStreams hits 0

	// optional hooks for test customization
	method1Hook func(ctx context.Context, in *grpccompat.In) (*grpccompat.Out, error)
}

func newEchoServer() *echoServer {
	return &echoServer{
		streamsClosed: make(chan struct{}),
	}
}

func (s *echoServer) incStreams() {
	s.mu.Lock()
	s.activeStreams++
	s.mu.Unlock()
}

func (s *echoServer) decStreams() {
	s.mu.Lock()
	s.activeStreams--
	if s.activeStreams == 0 {
		select {
		case <-s.streamsClosed:
		default:
			close(s.streamsClosed)
		}
	}
	s.mu.Unlock()
}

func (s *echoServer) getActiveStreams() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.activeStreams
}

// Method1 is a unary echo RPC.
func (s *echoServer) Method1(ctx context.Context, in *grpccompat.In) (*grpccompat.Out, error) {
	if s.method1Hook != nil {
		return s.method1Hook(ctx, in)
	}
	return &grpccompat.Out{Out: in.In, Buf: in.Buf}, nil
}

// Method2 is client-streaming: reads all inputs, returns count.
func (s *echoServer) Method2(stream grpccompat.DRPCService_Method2Stream) error {
	s.incStreams()
	defer s.decStreams()
	var count int64
	for {
		_, err := stream.Recv()
		if err != nil {
			break
		}
		count++
	}
	return stream.SendAndClose(&grpccompat.Out{Out: count})
}

// Method3 is server-streaming: sends In.In copies of the data back.
func (s *echoServer) Method3(in *grpccompat.In, stream grpccompat.DRPCService_Method3Stream) error {
	s.incStreams()
	defer s.decStreams()
	for i := int64(0); i < in.In; i++ {
		if err := stream.Send(&grpccompat.Out{Out: i, Buf: in.Buf}); err != nil {
			return err
		}
	}
	return nil
}

// Method4 is bidirectional: echoes each received message back.
func (s *echoServer) Method4(stream grpccompat.DRPCService_Method4Stream) error {
	s.incStreams()
	defer s.decStreams()
	for {
		msg, err := stream.Recv()
		if err != nil {
			return nil
		}
		if err := stream.Send(&grpccompat.Out{Out: msg.In, Buf: msg.Buf}); err != nil {
			return err
		}
	}
}
