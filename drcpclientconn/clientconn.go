package drcpclientconn

import (
	"context"
	"storj.io/drpc"
	"storj.io/drpc/drpcinterceptors"
)

type ClientConn struct {
	drpc.Conn
	dopts drpcinterceptors.DialOptions
}

// NewClientConnWithOptions creates a new ClientConn with the specified dial options
// and connection provider.
func NewClientConnWithOptions(dopts drpcinterceptors.DialOptions,
	connSupplier func() (drpc.Conn, error)) (*ClientConn, error) {

	conn, err := connSupplier()
	if err != nil {
		return nil, err
	}

	clientConn := &ClientConn{
		Conn:  conn,
		dopts: dopts,
	}

	clientConn.initInterceptors()
	return clientConn, nil
}

func (c ClientConn) Invoke(ctx context.Context, rpc string, enc drpc.Encoding, in, out drpc.Message) error {
	next := func(ctx context.Context, rpc string, in, out drpc.Message, enc drpc.Encoding) error {
		return c.Conn.Invoke(ctx, rpc, enc, in, out)
	}
	if c.dopts.UnaryInt != nil {
		return c.dopts.UnaryInt(ctx, rpc, in, out, c, enc, next)
	}
	return next(ctx, rpc, in, out, enc)
}

func (c ClientConn) NewStream(ctx context.Context, rpc string, enc drpc.Encoding) (drpc.Stream, error) {
	next := func(ctx context.Context, rpc string) (drpc.Stream, error) {
		return c.Conn.NewStream(ctx, rpc, enc)
	}
	if c.dopts.StreamInt != nil {
		return c.dopts.StreamInt(ctx, rpc, c, next)
	}
	return next(ctx, rpc)
}

func (c *ClientConn) initInterceptors() {
	c.dopts.ChainUnaryClientInterceptors()
	c.dopts.ChainStreamClientInterceptors()
}

var _ drpc.Conn = (*ClientConn)(nil)
