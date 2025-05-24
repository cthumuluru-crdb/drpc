package drcpclient

import (
	"context"

	"storj.io/drpc"
)

type ClientConn struct {
	drpc.Conn
	dopts *DialOptions
}

// NewClientConnWithOptions creates a new ClientConn with the specified dial options
// and connection provider.
func NewClientConnWithOptions(
	ctx context.Context, dialer func(ctx context.Context) (drpc.Conn, error), dialOpts ...DialOption,
) (*ClientConn, error) {
	conn, err := dialer(ctx)
	if err != nil {
		return nil, err
	}

	dopts := newDialOptions(dialOpts)
	clientConn := &ClientConn{
		Conn:  conn,
		dopts: dopts,
	}

	return clientConn, nil
}

func (c ClientConn) Invoke(ctx context.Context, rpc string, enc drpc.Encoding, in, out drpc.Message) error {
	invoker := c.Conn.Invoke
	if c.dopts.chainedUnaryInt != nil {
		return c.dopts.chainedUnaryInt(ctx, rpc, enc, in, out, c, invoker)
	}
	return invoker(ctx, rpc, enc, in, out)
}

func (c ClientConn) NewStream(ctx context.Context, rpc string, enc drpc.Encoding) (drpc.Stream, error) {
	streamer := c.Conn.NewStream
	if c.dopts.chainedStreamInt != nil {
		return c.dopts.chainedStreamInt(ctx, rpc, enc, c, streamer)
	}
	return streamer(ctx, rpc, enc)
}

var _ drpc.Conn = (*ClientConn)(nil)
