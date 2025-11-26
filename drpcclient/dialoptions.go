package drpcclient

import (
	"context"
	"crypto/tls"
	"math"
	"net"

	"github.com/google/uuid"
	"storj.io/drpc/drpcconn"
	"storj.io/drpc/drpcmanager"
	"storj.io/drpc/drpcstream"
	"storj.io/drpc/drpcwire"
)

// dialOptions configure a NewClientConnWithOptions call. dialOptions are set by the DialOption
// values passed to NewClientConnWithOptions.
type dialOptions struct {
	// Unary and stream interceptor chain.
	unaryInt  UnaryClientInterceptor
	streamInt StreamClientInterceptor

	// Unary and stream interceptors to be chained.
	unaryInts  []UnaryClientInterceptor
	streamInts []StreamClientInterceptor

	// RPC metadata to be added to each RPC call.
	perRPCMetadata map[string]string

	// dialer is an optional custom dialer function to use instead of default net.Dialer.
	dialer func(context.Context, string) (net.Conn, error)
	// tlsConfig is an optional TLS configuration for secure connections.
	tlsConfig *tls.Config
}

// DialOption configures how we set up the client connection.
type DialOption func(options *dialOptions)

func defaultDialOptions() dialOptions {
	return dialOptions{}
}

// WithChainUnaryInterceptor returns a DialOption that adds one or more unary RPC interceptors,
// chaining. Last interceptor is the innermost which eventually invokes the UnaryInvoker.
func WithChainUnaryInterceptor(ints ...UnaryClientInterceptor) DialOption {
	return func(opt *dialOptions) {
		opt.unaryInts = append(opt.unaryInts, ints...)
	}
}

// WithChainStreamInterceptor returns a DialOption that adds one or more stream RPC interceptors,
// chaining. Last interceptor is the innermost which eventually invokes the Streamer.
func WithChainStreamInterceptor(ints ...StreamClientInterceptor) DialOption {
	return func(opt *dialOptions) {
		opt.streamInts = append(opt.streamInts, ints...)
	}
}

func WithPerRPCMetadata(metadata map[string]string) DialOption {
	return func(opt *dialOptions) {
		opt.perRPCMetadata = metadata
	}
}

// WithTLSConfig returns a DialOption that sets the TLS configuration for
// secure connections.
func WithTLSConfig(tlsConfig *tls.Config) DialOption {
	return func(o *dialOptions) {
		o.tlsConfig = tlsConfig
	}
}

// WithContextDialer returns a DialOption that sets a custom dialer function
// to be used instead of the default net.Dialer.
func WithContextDialer(dialer func(context.Context, string) (net.Conn, error)) DialOption {
	return func(o *dialOptions) {
		o.dialer = dialer
	}
}

func DialContext(ctx context.Context, address string, opts ...DialOption) (*drpcconn.Conn, error) {
	var options dialOptions
	for _, opt := range opts {
		opt(&options)
	}

	netConn, err := func() (net.Conn, error) {
		if options.dialer != nil {
			return options.dialer(ctx, address)
		}
		dialer := &net.Dialer{}
		return dialer.DialContext(ctx, "tcp", address)
	}()
	if err != nil {
		return nil, err
	}

	if options.tlsConfig != nil {
		// Create a copy of the TLS configuration to prevent altering the
		// original copy.
		tlsConfig := options.tlsConfig.Clone()
		// Set the ServerName for TLS verification.
		sn, _, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		tlsConfig.ServerName = sn
		netConn = tls.Client(netConn, tlsConfig)
	}

	// TODO(chandrat): generate a conn ID to correlate the logs.
	id := uuid.New().String() // generate a unique connection ID.
	n, err := netConn.Write([]byte(id)[0:8])
	if err != nil || n < 8 {
		netConn.Close()
		return nil, err
	}

	return drpcconn.NewWithOptions(netConn, drpcconn.Options{
		Manager: drpcmanager.Options{
			Reader: drpcwire.ReaderOptions{
				MaximumBufferSize: math.MaxInt,
			},
			Stream: drpcstream.Options{
				MaximumBufferSize: 0, // unlimited
			},
			ConnID:     string(id[0:8]),
			SoftCancel: true, // don't close the transport when stream context is canceled
		},
	}), nil
}
