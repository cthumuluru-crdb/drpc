// Copyright (C) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package drpcconn

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/zeebo/assert"
	grpcmetadata "google.golang.org/grpc/metadata"
	"storj.io/drpc"
	"storj.io/drpc/drpcmetadata"
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

func TestConn_InvokeFlushesSendClose(t *testing.T) {
	ctx := drpctest.NewTracker(t)
	defer ctx.Close()

	pc, ps := net.Pipe()
	defer func() { assert.NoError(t, pc.Close()) }()
	defer func() { assert.NoError(t, ps.Close()) }()

	invokeDone := make(chan struct{})

	ctx.Run(func(ctx context.Context) {
		wr := drpcwire.NewWriter(ps, 64)
		rd := drpcwire.NewReader(ps)

		_, _ = rd.ReadFrame()    // Invoke
		_, _ = rd.ReadFrame()    // Message
		pkt, _ := rd.ReadFrame() // CloseSend

		_ = wr.WritePacket(drpcwire.Packet{
			Data: []byte("qux"),
			ID:   drpcwire.ID{Stream: pkt.ID.Stream, Message: 1},
			Kind: drpcwire.KindMessage,
		})
		_ = wr.Flush()

		_, _ = rd.ReadFrame() // Close
		<-invokeDone          // wait for invoke to return

		// ensure that any later packets are dropped by writing one
		// before closing the transport.
		for i := 0; i < 5; i++ {
			_ = wr.WritePacket(drpcwire.Packet{
				ID:   drpcwire.ID{Stream: pkt.ID.Stream, Message: 2},
				Kind: drpcwire.KindCloseSend,
			})
			_ = wr.Flush()
		}

		_ = ps.Close()
	})

	conn := New(pc)

	in, out := "baz", ""
	assert.NoError(t, conn.Invoke(ctx, "/com.example.Foo/Bar", testEncoding{}, &in, &out))
	assert.True(t, out == "qux")

	invokeDone <- struct{}{} // signal invoke has returned

	// we should eventually notice the transport is closed
	select {
	case <-conn.Closed():
	case <-time.After(1 * time.Second):
		t.Fatal("took too long for conn to be closed")
	}
}

func TestConn_InvokeSendsGrpcAndDrpcMetadata(t *testing.T) {
	ctx := drpctest.NewTracker(t)
	defer ctx.Close()

	pc, ps := net.Pipe()
	defer func() { assert.NoError(t, pc.Close()) }()
	defer func() { assert.NoError(t, ps.Close()) }()

	ctx.Run(func(ctx context.Context) {
		wr := drpcwire.NewWriter(ps, 64)
		rd := drpcwire.NewReader(ps)

		md, err := rd.ReadFrame() // Metadata
		assert.NoError(t, err)
		assert.Equal(t, md.Kind, drpcwire.KindInvokeMetadata)
		metadata, err := drpcmetadata.Decode(md.Data)
		assert.NoError(t, err)
		assert.Equal(t, metadata, map[string]string{
			"grpc-key":             "grpc-value",
			"drpc-key":             "drpc-value",
			"grpc-multi-value-key": "grpc-value1",
			"common-key":           "common-value2",
		})

		_, _ = rd.ReadFrame()    // Invoke
		_, _ = rd.ReadFrame()    // Message
		pkt, _ := rd.ReadFrame() // CloseSend

		_ = wr.WritePacket(drpcwire.Packet{
			Data: []byte("qux"),
			ID:   drpcwire.ID{Stream: pkt.ID.Stream, Message: 1},
			Kind: drpcwire.KindMessage,
		})
		_ = wr.Flush()

		_, _ = rd.ReadFrame() // Close
	})

	conn := New(pc)

	in, out := "baz", ""
	ctx.Context = grpcmetadata.NewOutgoingContext(ctx.Context,
		grpcmetadata.MD{
			"grpc-key":             []string{"grpc-value"},
			"grpc-multi-value-key": []string{"grpc-value1", "grpc-value2"},
			"common-key":           []string{"common-value1"},
		},
	)
	ctx.Context = drpcmetadata.AppendToOutgoingContext(ctx.Context,
		map[string]string{
			"drpc-key":   "drpc-value",
			"common-key": "common-value2",
		},
	)
	assert.NoError(t, conn.Invoke(ctx, "/com.example.Foo/Bar", testEncoding{}, &in, &out))
}

func TestConn_NewStreamSendsGrpcAndDrpcMetadata(t *testing.T) {
	ctx := drpctest.NewTracker(t)

	pc, ps := net.Pipe()
	defer func() { _ = pc.Close() }()
	defer func() { _ = ps.Close() }()
	// Wait for goroutines before closing pipes. With async writes,
	// the writer goroutine may not have flushed to the pipe yet.
	defer ctx.Close()

	ctx.Run(func(ctx context.Context) {
		rd := drpcwire.NewReader(ps)

		md, err := rd.ReadFrame() // Metadata
		assert.NoError(t, err)
		assert.Equal(t, md.Kind, drpcwire.KindInvokeMetadata)
		metadata, err := drpcmetadata.Decode(md.Data)
		assert.NoError(t, err)
		assert.Equal(t, metadata, map[string]string{
			"grpc-key": "grpc-value",
			"drpc-key": "drpc-value",
		})

		_, _ = rd.ReadFrame() // Invoke
		_, _ = rd.ReadFrame() // CloseSend
	})

	conn := New(pc)

	ctx.Context = grpcmetadata.NewOutgoingContext(ctx.Context,
		grpcmetadata.MD{
			"grpc-key": []string{"grpc-value"},
		},
	)
	ctx.Context = drpcmetadata.AppendToOutgoingContext(ctx.Context, map[string]string{
		"drpc-key": "drpc-value",
	})
	s, err := conn.NewStream(ctx, "/com.example.Foo/Bar", testEncoding{})
	assert.NoError(t, err)
	_ = s.CloseSend()
}

func TestConn_ConcurrentInvoke(t *testing.T) {
	ctx := drpctest.NewTracker(t)
	defer ctx.Close()

	pc, ps := net.Pipe()
	defer func() { _ = pc.Close() }()
	defer func() { _ = ps.Close() }()

	// slowReady is closed when the slow handler has received the request,
	// signaling that the fast handler can proceed.
	slowReady := make(chan struct{})

	// Server goroutine: handle two concurrent streams. The first stream
	// (slow) blocks until the second (fast) completes its response.
	ctx.Run(func(ctx context.Context) {
		rd := drpcwire.NewReader(ps)
		wr := drpcwire.NewWriter(ps, 1024)

		// Read slow stream: Invoke + Message + CloseSend
		inv1, _ := rd.ReadFrame() // Invoke (stream 1)
		msg1, _ := rd.ReadFrame() // Message (stream 1)
		_, _ = rd.ReadFrame()     // CloseSend (stream 1)
		_ = msg1

		close(slowReady)

		// Read fast stream: Invoke + Message + CloseSend
		inv2, _ := rd.ReadFrame() // Invoke (stream 2)
		msg2, _ := rd.ReadFrame() // Message (stream 2)
		_, _ = rd.ReadFrame()     // CloseSend (stream 2)
		_ = msg2

		// Respond to fast stream first.
		_ = wr.WritePacket(drpcwire.Packet{
			Data: []byte("fast-reply"),
			ID:   drpcwire.ID{Stream: inv2.ID.Stream, Message: 1},
			Kind: drpcwire.KindMessage,
		})
		_ = wr.Flush()

		// Now respond to slow stream.
		_ = wr.WritePacket(drpcwire.Packet{
			Data: []byte("slow-reply"),
			ID:   drpcwire.ID{Stream: inv1.ID.Stream, Message: 1},
			Kind: drpcwire.KindMessage,
		})
		_ = wr.Flush()

		// Read close frames.
		_, _ = rd.ReadFrame() // Close (fast)
		_, _ = rd.ReadFrame() // Close (slow)
	})

	conn := New(pc)

	var wg sync.WaitGroup
	var slowOut, fastOut string
	var slowErr, fastErr error

	// Launch slow invoke.
	wg.Add(1)
	go func() {
		defer wg.Done()
		in := "slow-req"
		slowErr = conn.Invoke(ctx, "/svc/Slow", testEncoding{}, &in, &slowOut)
	}()

	// Wait for slow request to reach the server, then launch fast invoke.
	<-slowReady
	wg.Add(1)
	go func() {
		defer wg.Done()
		in := "fast-req"
		fastErr = conn.Invoke(ctx, "/svc/Fast", testEncoding{}, &in, &fastOut)
	}()

	wg.Wait()

	assert.NoError(t, slowErr)
	assert.NoError(t, fastErr)
	assert.Equal(t, slowOut, "slow-reply")
	assert.Equal(t, fastOut, "fast-reply")
}

func TestConn_encodeMetadata(t *testing.T) {
	pc, ps := net.Pipe()
	defer func() { assert.NoError(t, pc.Close()) }()
	defer func() { assert.NoError(t, ps.Close()) }()

	conn := New(pc)

	t.Run("no-metadata", func(t *testing.T) {
		ctx := context.Background()

		metadata, err := conn.encodeMetadata(ctx)
		assert.NoError(t, err)
		decodedMd, err := drpcmetadata.Decode(metadata)
		assert.NoError(t, err)
		assert.Equal(t, decodedMd, map[string]string(nil))
	})

	t.Run("grpc-only", func(t *testing.T) {
		ctx := context.Background()

		ctx = grpcmetadata.NewOutgoingContext(ctx,
			grpcmetadata.MD{
				"grpc-key":                   []string{"grpc-value"},
				"grpc-multi-value-key":       []string{"grpc-value1", "grpc-value2"},
				"grpc-key-with-empty-slice":  []string{},
				"grpc-key-with-empty-string": []string{""},
			},
		)

		metadata, err := conn.encodeMetadata(ctx)
		assert.NoError(t, err)
		decodedMd, err := drpcmetadata.Decode(metadata)
		assert.NoError(t, err)
		assert.Equal(t, decodedMd, map[string]string{
			"grpc-key":                   "grpc-value",
			"grpc-multi-value-key":       "grpc-value1",
			"grpc-key-with-empty-string": "",
		})
	})

	t.Run("drpc-only", func(t *testing.T) {
		ctx := context.Background()

		ctx = drpcmetadata.AppendToOutgoingContext(ctx,
			map[string]string{
				"drpc-key":                   "drpc-value",
				"drpc-key-with-empty-string": "",
			})

		metadata, err := conn.encodeMetadata(ctx)
		assert.NoError(t, err)
		decodedMd, err := drpcmetadata.Decode(metadata)
		assert.NoError(t, err)
		assert.Equal(t, decodedMd, map[string]string{
			"drpc-key":                   "drpc-value",
			"drpc-key-with-empty-string": ""})
	})

	t.Run("grpc-and-drpc", func(t *testing.T) {
		ctx := context.Background()

		ctx = grpcmetadata.NewOutgoingContext(ctx,
			grpcmetadata.MD{
				"grpc-key":             []string{"grpc-value"},
				"grpc-multi-value-key": []string{"grpc-value1", "grpc-value2"},
				"common-key1":          []string{"common-value1"},
				"common-key2":          []string{"common-value"},
			},
		)
		ctx = drpcmetadata.AppendToOutgoingContext(ctx,
			map[string]string{
				"drpc-key":    "drpc-value",
				"common-key1": "common-value2",
				"common-key2": "common-value",
			},
		)
		metadata, err := conn.encodeMetadata(ctx)
		assert.NoError(t, err)
		decodedMd, err := drpcmetadata.Decode(metadata)
		assert.NoError(t, err)
		assert.Equal(t, decodedMd, map[string]string{
			"grpc-key":             "grpc-value",
			"drpc-key":             "drpc-value",
			"grpc-multi-value-key": "grpc-value1",
			"common-key1":          "common-value2",
			"common-key2":          "common-value",
		})
	})
}
