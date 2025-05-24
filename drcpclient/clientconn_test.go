package drcpclient

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"storj.io/drpc"
	"storj.io/drpc/drpcconn"
	"storj.io/drpc/drpcpool"
	"storj.io/drpc/drpctest"
	"storj.io/drpc/drpcwire"
)

// Dummy encoding, which assumes the drpc.Message is a *string.
type testEncoding struct{}

func (testEncoding) Marshal(msg drpc.Message) ([]byte, error) {
	return []byte(*msg.(*string)), nil
}

func (testEncoding) Unmarshal(buf []byte, msg drpc.Message) error {
	*msg.(*string) = string(buf)
	return nil
}

// TestUnaryInterceptorChainWithPooledAndConcreteDrpcConn verifies that unary interceptors
// work correctly with both direct drpcconn.Conn and pooled drpcpool.Conn connection types.
//
// This test ensures that:
// 1. The interceptor chain executes properly in both connection scenarios
// 2. Interceptors are called in the correct order (first-to-last on the way in, last-to-first on the way out)
// 3. The RPC payload is correctly transmitted through the interceptor chain
func TestUnaryInterceptorChainWithPooledAndConcreteDrpcConn(t *testing.T) {

	// Test cases for different connection supplier implementations
	testCases := []struct {
		name           string
		createSupplier func(context.Context, net.Conn) func(context.Context) (drpc.Conn, error)
	}{
		{
			name: "drpcconn.Conn supplier",
			// Basic connection supplier that returns a concrete connection directly
			createSupplier: func(ctx context.Context, pc net.Conn) func(ctx context.Context) (drpc.Conn, error) {
				return func(_ context.Context) (drpc.Conn, error) {
					return drpcconn.New(pc), nil
				}
			},
		},
		{
			name: "drpcpool.Conn supplier",
			// Pool-based connection supplier that returns connections from a connection pool
			createSupplier: func(ctx context.Context, pc net.Conn) func(ctx context.Context) (drpc.Conn, error) {
				pool := drpcpool.New[string, drpcpool.Conn](drpcpool.Options{
					Capacity:    2,
					KeyCapacity: 1,
					Expiration:  time.Minute,
				})
				t.Cleanup(func() {
					err := pool.Close()
					require.NoError(t, err)
				})
				return func(ctx context.Context) (drpc.Conn, error) {
					// Get a connection from the pool using a test server address
					return pool.Get(ctx, "test.server:8080", func(ctx context.Context, addr string) (drpcpool.Conn, error) {
						return drpcconn.New(pc), nil
					}), nil
				}
			},
		},
	}

	for _, tc := range testCases {
		ctx := drpctest.NewTracker(t)
		pc, ps := net.Pipe() // client and server side of pipe respectively
		defer func() {
			err := pc.Close()
			require.NoError(t, err)
		}()

		defer func() {
			err := ps.Close()
			require.NoError(t, err)
		}()

		dialer := tc.createSupplier(ctx, pc)

		var interceptorCalls []string = []string{}

		interceptor1 := func(ctx context.Context, method string, enc drpc.Encoding, in, out drpc.Message,
			conn drpc.Conn, invoker UnaryInvoker) error {
			interceptorCalls = append(interceptorCalls, "interceptor1_before")
			err := invoker(ctx, method, enc, in, out)
			interceptorCalls = append(interceptorCalls, "interceptor1_after")
			return err
		}

		interceptor2 := func(ctx context.Context, method string, enc drpc.Encoding, in, out drpc.Message,
			conn drpc.Conn, invoker UnaryInvoker) error {
			interceptorCalls = append(interceptorCalls, "interceptor2_before")
			err := invoker(ctx, method, enc, in, out)
			interceptorCalls = append(interceptorCalls, "interceptor2_after")
			return err
		}

		// Configure the dial options with a chain of two interceptors
		dialOpts := []DialOption{
			WithChainUnaryInterceptor(interceptor1, interceptor2),
		}
		// Create a client connection with the configured options and connection supplier
		cc, err := NewClientConnWithOptions(ctx, dialer, dialOpts...)
		require.NoError(t, err)

		in, out := "input", ""
		done := make(chan struct{})

		// Simulation of a server handling the RPC request
		ctx.Run(func(ctx context.Context) {
			rd := drpcwire.NewReader(ps)
			wr := drpcwire.NewWriter(ps, 64)
			_, _ = rd.ReadPacket()    // Invoke
			_, _ = rd.ReadPacket()    // Message
			pkt, _ := rd.ReadPacket() // CloseSend

			_ = wr.WritePacket(drpcwire.Packet{
				Data: []byte("output"),
				ID:   drpcwire.ID{Stream: pkt.ID.Stream, Message: 1},
				Kind: drpcwire.KindMessage,
			})
			_ = wr.Flush()
			_, _ = rd.ReadPacket() // Close
			close(done)
		})

		err = cc.Invoke(ctx, "TestMethod", testEncoding{}, &in, &out)
		require.NoError(t, err)
		// Verify the output matches what the server sent
		assert.Equal(t, "output", out)

		// Check the order of interceptor calls
		expected := []string{
			"interceptor1_before",
			"interceptor2_before",
			"interceptor2_after",
			"interceptor1_after",
		}
		assert.Equal(t, expected, interceptorCalls)

		<-done
	}

}
