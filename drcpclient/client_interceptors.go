package drcpclient

import (
	"context"

	"storj.io/drpc"
)

type UnaryClientInterceptor func(ctx context.Context, rpc string, enc drpc.Encoding, in, out drpc.Message, cc drpc.Conn, next UnaryInvoker) error

type UnaryInvoker func(ctx context.Context, rpc string, enc drpc.Encoding, in, out drpc.Message) error

// Streamer is a function that opens a new DRPC stream.
type Streamer func(ctx context.Context, rpc string, enc drpc.Encoding) (drpc.Stream, error)

// StreamClientInterceptor is the DRPC equivalent of a gRPC stream client interceptor.
type StreamClientInterceptor func(ctx context.Context, rpc string, enc drpc.Encoding, conn drpc.Conn, streamer Streamer) (drpc.Stream, error)

type DialOptions struct {
	chainedUnaryInt  UnaryClientInterceptor
	unaryInts        []UnaryClientInterceptor
	chainedStreamInt StreamClientInterceptor
	streamInts       []StreamClientInterceptor
}
type DialOption func(options *DialOptions)

func WithChainUnaryInterceptor(ints ...UnaryClientInterceptor) DialOption {
	return func(opt *DialOptions) {
		opt.unaryInts = append(opt.unaryInts, ints...)
	}
}

func WithChainStreamInterceptor(ints ...StreamClientInterceptor) DialOption {
	return func(opt *DialOptions) {
		opt.streamInts = append(opt.streamInts, ints...)
	}
}

func newDialOptions(opts []DialOption) *DialOptions {
	options := &DialOptions{}
	for _, opt := range opts {
		opt(options)
	}
	options.chainUnaryClientInterceptors()
	options.chainStreamClientInterceptors()

	return options
}

// chainUnaryClientInterceptors chains all unary client interceptors in the DialOptions into a single interceptor.
//
// This method inspects the slice of unary client interceptors (`d.unaryInts`) and combines them into one interceptor,
// assigning the result to `d.UnaryInt`. If there are no interceptors, `d.UnaryInt` is set to nil. If there is only one,
// it is used directly. If there are multiple, they are chained so that each interceptor can process the request and
// pass control to the next, ending with the original invoker.
//
// The interceptors are invoked in the order they were added.
//
// Example usage:
//
//	opts := drpcinterceptors.NewDialOptions([]drpcinterceptors.DialOption{
//		drpcinterceptors.WithChainStreamInterceptor(loggingInterceptor, metricsInterceptor),
//	})
//	opts.ChainUnaryClientInterceptors()
//	// opts.UnaryInt now contains the chained interceptor.
//
// Side effects:
//   - Sets d.UnaryInt to the chained interceptor or nil.
func (d *DialOptions) chainUnaryClientInterceptors() {
	// NB: gRPC appears to treat unary interceptor special (WithUnaryInterceptor).
	// Either we have to design out interceptors similarly or change gRPC consumers to use chained interceptors.
	// (chandrat) I prefer the former.
	n := len(d.unaryInts)
	d.chainedUnaryInt = func(ctx context.Context, rpc string, enc drpc.Encoding, in, out drpc.Message, conn drpc.Conn, invoker UnaryInvoker) error {
		chained := invoker
		for i := n - 1; i >= 0; i-- {
			next := chained
			interceptor := d.unaryInts[i]
			chained = func(ctx context.Context, rpc string, enc drpc.Encoding, in, out drpc.Message) error {
				return interceptor(ctx, rpc, enc, in, out, conn, next)
			}
		}
		return chained(ctx, rpc, enc, in, out)
	}
}

// chainStreamClientInterceptors chains all stream client interceptors in the DialOptions into a single interceptor.
//
// This method examines the slice of stream client interceptors (`d.streamInts`) and combines them into one interceptor,
// assigning the result to `d.StreamInt`. If there are no interceptors, `d.StreamInt` is set to nil. If there is only one,
// it is used directly. If there are multiple, they are chained so that each interceptor can process the stream request
// and pass control to the next, ending with the original streamer.
//
// The interceptors are invoked in the order they were added.
//
// Example usage:
//
//	opts := drpcinterceptors.NewDialOptions([]drpcinterceptors.DialOption{
//		drpcinterceptors.WithChainStreamInterceptor(loggingInterceptor, metricsInterceptor),
//	})
//	opts.chainStreamClientInterceptors()
//	// opts.StreamInt now contains the chained stream interceptor.
//
// Side effects:
//   - Sets d.StreamInt to the chained interceptor or nil.
func (d *DialOptions) chainStreamClientInterceptors() {
	n := len(d.unaryInts)
	d.chainedStreamInt = func(ctx context.Context, rpc string, enc drpc.Encoding, conn drpc.Conn, streamer Streamer) (drpc.Stream, error) {
		chained := streamer
		for i := n - 1; i >= 0; i-- {
			next := chained
			interceptor := d.streamInts[i]
			chained = func(ctx context.Context, rpc string, enc drpc.Encoding) (drpc.Stream, error) {
				return interceptor(ctx, rpc, enc, conn, next)
			}
		}
		return chained(ctx, rpc, enc)
	}
}
