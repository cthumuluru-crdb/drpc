package simnet

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"

	rpc "github.com/glycerine/rpc25519"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"storj.io/drpc/internal/grpccompat"
)

// grpcSimnetEnv is the gRPC equivalent of simnetEnv. It wires a gRPC
// server and client over the same gosimnet transport, providing a
// baseline for comparison with DRPC under identical fault conditions.
type grpcSimnetEnv struct {
	t       testing.TB
	seed    int64
	srvName string
	cliName string
	cfg     *rpc.Config
	srv     *rpc.Server
	cli     *rpc.Client
	simnet  *rpc.Simnet

	// gRPC layer
	grpcSrv    *grpc.Server
	grpcConn   *grpc.ClientConn
	grpcClient grpccompat.ServiceClient

	// net.Conn tracking for cleanup and transport-close tests
	clientNetConn net.Conn
	serverConn    net.Conn
	serverConnMu  sync.Mutex

	// tracks when gRPC server exits
	serveDone chan struct{}
}

// grpcEchoServer implements grpccompat.ServiceServer with echo behavior,
// mirroring the DRPC echoServer for apples-to-apples comparison.
type grpcEchoServer struct {
	grpccompat.UnimplementedServiceServer

	mu            sync.Mutex
	activeStreams int
	streamsClosed chan struct{}
}

func newGRPCEchoServer() *grpcEchoServer {
	return &grpcEchoServer{
		streamsClosed: make(chan struct{}),
	}
}

func (s *grpcEchoServer) incStreams() {
	s.mu.Lock()
	s.activeStreams++
	s.mu.Unlock()
}

func (s *grpcEchoServer) decStreams() {
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

func (s *grpcEchoServer) Method1(ctx context.Context, in *grpccompat.In) (*grpccompat.Out, error) {
	return &grpccompat.Out{Out: in.In, Buf: in.Buf}, nil
}

func (s *grpcEchoServer) Method2(stream grpccompat.Service_Method2Server) error {
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

func (s *grpcEchoServer) Method3(in *grpccompat.In, stream grpccompat.Service_Method3Server) error {
	s.incStreams()
	defer s.decStreams()
	for i := int64(0); i < in.In; i++ {
		if err := stream.Send(&grpccompat.Out{Out: i, Buf: in.Buf}); err != nil {
			return err
		}
	}
	return nil
}

func (s *grpcEchoServer) Method4(stream grpccompat.Service_Method4Server) error {
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

// newGRPCSimnetEnv creates a gosimnet environment and wires gRPC on top.
func newGRPCSimnetEnv(
	t testing.TB, seed int64, serverImpl grpccompat.ServiceServer,
) *grpcSimnetEnv {
	t.Helper()

	id := envCounter.Add(1)

	cfg := rpc.NewConfig()
	cfg.UseSimNet = true
	cfg.InitialSimnetScenario = seed
	cfg.ServerAddr = "127.0.0.1:0"

	srvName := fmt.Sprintf("grpc_srv_%s_%d", t.Name(), id)
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

	// Set up gRPC server
	grpcSrv := grpc.NewServer()
	grpccompat.RegisterServiceServer(grpcSrv, serverImpl)

	cliName := fmt.Sprintf("grpc_cli_%s_%d", t.Name(), id)

	env := &grpcSimnetEnv{
		t:         t,
		seed:      seed,
		srvName:   srvName,
		cliName:   cliName,
		cfg:       cfg,
		srv:       srv,
		simnet:    simnet,
		grpcSrv:   grpcSrv,
		serveDone: make(chan struct{}),
	}

	// Serve gRPC on the simnet listener, capturing server-side conns.
	wrappedLsn := &connCaptureLsn{Listener: lsn, env: env}
	go func() {
		defer close(env.serveDone)
		_ = grpcSrv.Serve(wrappedLsn)
	}()

	// Dial the client side through simnet
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

	env.clientNetConn = clientConn

	// Wrap the simnet net.Conn for gRPC client usage.
	// gRPC needs grpc.NewClient with a custom dialer that returns our conn.
	grpcConn, err2 := grpc.NewClient("passthrough:///simnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return clientConn, nil
		}),
	)
	if err2 != nil {
		t.Fatalf("grpc.NewClient: %v", err2)
	}
	env.grpcConn = grpcConn
	env.grpcClient = grpccompat.NewServiceClient(grpcConn)

	return env
}

// close tears down the gRPC environment.
func (e *grpcSimnetEnv) close() {
	if e.grpcConn != nil {
		_ = e.grpcConn.Close()
	}
	if e.grpcSrv != nil {
		e.grpcSrv.GracefulStop()
	}
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

// connCaptureLsn wraps a net.Listener to capture accepted connections
// into grpcSimnetEnv.serverConn for use in close/fault tests.
type connCaptureLsn struct {
	net.Listener
	env *grpcSimnetEnv
}

func (l *connCaptureLsn) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.env.serverConnMu.Lock()
	l.env.serverConn = c
	l.env.serverConnMu.Unlock()
	return c, nil
}
