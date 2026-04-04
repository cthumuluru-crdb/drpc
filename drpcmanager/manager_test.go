// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package drpcmanager

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/zeebo/assert"
	grpcmetadata "google.golang.org/grpc/metadata"
	"storj.io/drpc"
	"storj.io/drpc/drpcmetadata"
	"storj.io/drpc/drpctest"
	"storj.io/drpc/drpcwire"
)

func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestTimeout(t *testing.T) {
	tr := make(blockingTransport)
	man := NewWithOptions(tr, Options{
		InactivityTimeout: time.Millisecond,
	})
	defer func() { _ = man.Close() }()

	_, _, err := man.NewServerStream(context.Background())
	assert.That(t, errors.Is(err, context.DeadlineExceeded))
}

func TestDrpcMetadata(t *testing.T) {
	ctx := drpctest.NewTracker(t)
	defer ctx.Close()

	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	cman := New(cconn)
	defer func() { _ = cman.Close() }()

	sman := NewWithOptions(sconn, Options{
		GRPCMetadataCompatMode: false,
	})
	defer func() { _ = sman.Close() }()

	ctx.Run(func(ctx context.Context) {
		stream, err := cman.NewClientStream(ctx, "rpc")
		assert.NoError(t, err)
		defer func() { _ = stream.Close() }()

		md := map[string]string{"key": "value", "multi-value-key": "value1,value2"}
		var buf []byte
		buf, err = drpcmetadata.Encode(buf, md)
		assert.NoError(t, err)
		assert.NoError(t, stream.RawWrite(drpcwire.KindInvokeMetadata, buf))
		assert.NoError(t, stream.RawWrite(drpcwire.KindInvoke, []byte("invoke")))
		assert.NoError(t, stream.RawWrite(drpcwire.KindMessage, []byte("message")))
		assert.NoError(t, stream.RawFlush())
		assert.NoError(t, stream.Close())
	})

	ctx.Run(func(ctx context.Context) {
		stream, _, err := sman.NewServerStream(ctx)
		assert.NoError(t, err)
		streamCtx := stream.Context()

		drpcMd, ok := drpcmetadata.GetFromIncomingContext(streamCtx)
		assert.That(t, ok)
		assert.Equal(t, drpcMd, map[string]string{"key": "value", "multi-value-key": "value1,value2"})

		grpcMd, ok := grpcmetadata.FromIncomingContext(streamCtx)
		assert.False(t, ok)
		assert.Nil(t, grpcMd)

		defer func() { _ = stream.Close() }()

		_, err = stream.RawRecv()
		assert.NoError(t, err)

		_, err = stream.RawRecv()
		assert.That(t, errors.Is(err, io.EOF))
	})

	ctx.Wait()
}

func TestDrpcMetadataWithGRPCMetadataCompatMode(t *testing.T) {
	ctx := drpctest.NewTracker(t)
	defer ctx.Close()

	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	cman := New(cconn)
	defer func() { _ = cman.Close() }()

	sman := NewWithOptions(sconn, Options{
		GRPCMetadataCompatMode: true,
	})
	defer func() { _ = sman.Close() }()

	ctx.Run(func(ctx context.Context) {
		stream, err := cman.NewClientStream(ctx, "rpc")
		assert.NoError(t, err)
		defer func() { _ = stream.Close() }()

		md := map[string]string{"key": "value", "multi-value-key": "value1,value2"}
		var buf []byte
		buf, err = drpcmetadata.Encode(buf, md)
		assert.NoError(t, err)
		assert.NoError(t, stream.RawWrite(drpcwire.KindInvokeMetadata, buf))
		assert.NoError(t, stream.RawWrite(drpcwire.KindInvoke, []byte("invoke")))
		assert.NoError(t, stream.RawWrite(drpcwire.KindMessage, []byte("message")))
		assert.NoError(t, stream.RawFlush())
		assert.NoError(t, stream.Close())
	})

	ctx.Run(func(ctx context.Context) {
		stream, _, err := sman.NewServerStream(ctx)
		assert.NoError(t, err)
		streamCtx := stream.Context()

		drpcMd, ok := drpcmetadata.GetFromIncomingContext(streamCtx)
		assert.False(t, ok)
		assert.Nil(t, drpcMd)

		grpcMd, ok := grpcmetadata.FromIncomingContext(streamCtx)
		assert.That(t, ok)
		assert.Equal(t, grpcMd, grpcmetadata.MD{"key": []string{"value"},
			"multi-value-key": []string{"value1,value2"}})

		defer func() { _ = stream.Close() }()

		_, err = stream.RawRecv()
		assert.NoError(t, err)

		_, err = stream.RawRecv()
		assert.That(t, errors.Is(err, io.EOF))
	})

	ctx.Wait()
}

// writeFrames serializes the given frames and writes them to w.
func writeFrames(t *testing.T, w io.Writer, frames ...drpcwire.Frame) {
	t.Helper()
	var buf []byte
	for _, fr := range frames {
		buf = drpcwire.AppendFrame(buf, fr)
	}
	_, err := w.Write(buf)
	assert.NoError(t, err)
}

// createFrame is a shorthand for constructing a Frame.
func createFrame(kind drpcwire.Kind, sid, mid uint64, data string, done bool) drpcwire.Frame {
	return drpcwire.Frame{
		ID:   drpcwire.ID{Stream: sid, Message: mid},
		Kind: kind,
		Data: []byte(data),
		Done: done,
	}
}

// waitForClosed blocks until the manager terminates or the timeout expires.
func waitForClosed(t *testing.T, man *Manager) {
	t.Helper()
	select {
	case <-man.Closed():
	case <-time.After(5 * time.Second):
		t.Fatal("manager did not terminate in time")
	}
}

//
// manageReader tests
//

// Within a single stream, message IDs must be monotonically increasing.
// The stream's own PacketAssembler enforces this, causing the manager to
// terminate with a protocol error.
func TestManageReader_GlobalMonotonicity_SameStream(t *testing.T) {
	ctx := drpctest.NewTracker(t)
	defer ctx.Close()

	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	man := New(sconn)
	defer func() { _ = man.Close() }()

	// Consume the invoke and drain messages so HandleFrame doesn't block.
	ctx.Run(func(ctx context.Context) {
		stream, _, err := man.NewServerStream(ctx)
		assert.NoError(t, err)
		for {
			if _, err := stream.RawRecv(); err != nil {
				return
			}
		}
	})

	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 1, 1, "rpc", true),
		createFrame(drpcwire.KindMessage, 1, 5, "ok", true),
		createFrame(drpcwire.KindMessage, 1, 4, "bad", true),
	)

	waitForClosed(t, man)
}

// Cross-stream frames: after a stream is removed, frames for that stream ID
// are silently ignored and the manager stays alive.
func TestManageReader_CrossStreamFramesIgnored(t *testing.T) {
	ctx := drpctest.NewTracker(t)
	defer ctx.Close()

	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	man := NewWithOptions(sconn, Options{SoftCancel: true})
	defer func() { _ = man.Close() }()

	// Drain client-side writes.
	ctx.Run(func(ctx context.Context) {
		buf := make([]byte, 4096)
		for {
			if _, err := cconn.Read(buf); err != nil {
				return
			}
		}
	})

	// Create stream 1 with a cancelable context, then cancel it.
	subctx, cancel := context.WithCancel(ctx)
	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 1, 1, "rpc1", true),
	)
	stream1, _, err := man.NewServerStream(subctx)
	assert.NoError(t, err)
	cancel()
	<-stream1.Finished()

	// Send a frame for the now-removed stream 1, then a valid invoke for
	// stream 2. The stale frame should be silently dropped.
	recv := make(chan []byte, 1)
	ctx.Run(func(ctx context.Context) {
		stream2, _, err := man.NewServerStream(ctx)
		assert.NoError(t, err)
		data, err := stream2.RawRecv()
		assert.NoError(t, err)
		recv <- data
	})

	writeFrames(t, cconn,
		createFrame(drpcwire.KindMessage, 1, 4, "stale", true),
		createFrame(drpcwire.KindInvoke, 2, 1, "rpc2", true),
		createFrame(drpcwire.KindMessage, 2, 2, "fresh", true),
	)

	assert.DeepEqual(t, <-recv, []byte("fresh"))
}

// Invoke stream ID regression: after invoking stream 2, an invoke for stream 1
// is rejected by the invoke stream ID monotonicity check.
func TestManageReader_InvokeStreamIDRegression(t *testing.T) {
	ctx := drpctest.NewTracker(t)
	defer ctx.Close()

	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	man := NewWithOptions(sconn, Options{SoftCancel: true})
	defer func() { _ = man.Close() }()

	// Drain client-side writes.
	ctx.Run(func(ctx context.Context) {
		buf := make([]byte, 4096)
		for {
			if _, err := cconn.Read(buf); err != nil {
				return
			}
		}
	})

	// Create and cancel stream 2.
	subctx, cancel := context.WithCancel(ctx)
	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 2, 1, "rpc2", true),
	)
	stream2, _, err := man.NewServerStream(subctx)
	assert.NoError(t, err)
	cancel()
	<-stream2.Finished()

	// Now send an invoke for stream 1 (lower than 2). This should terminate
	// the manager with a protocol error.
	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 1, 1, "rpc1", true),
	)

	waitForClosed(t, man)
}

// Invoke replay: a second invoke for the same stream ID is delivered to the
// active stream, which rejects it as "invoke on existing stream".
func TestManageReader_InvokeReplayBlocked(t *testing.T) {
	ctx := drpctest.NewTracker(t)
	defer ctx.Close()

	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	man := New(sconn)
	defer func() { _ = man.Close() }()

	ctx.Run(func(ctx context.Context) {
		_, _, _ = man.NewServerStream(ctx)
	})

	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 1, 1, "rpc", true),
		createFrame(drpcwire.KindInvoke, 1, 1, "rpc", true),
	)

	waitForClosed(t, man)
}

// Non-done frames don't bump the message ID, so continuation frames with
// the same ID are accepted.
func TestManageReader_ContinuationFramesAccepted(t *testing.T) {
	ctx := drpctest.NewTracker(t)
	defer ctx.Close()

	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	man := New(sconn)
	defer func() { _ = man.Close() }()

	recv := make(chan []byte, 1)
	ctx.Run(func(ctx context.Context) {
		stream, _, err := man.NewServerStream(ctx)
		assert.NoError(t, err)
		data, err := stream.RawRecv()
		assert.NoError(t, err)
		recv <- data
	})

	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 1, 1, "rpc", true),
		createFrame(drpcwire.KindMessage, 1, 2, "hel", false),
		createFrame(drpcwire.KindMessage, 1, 2, "lo", true),
	)

	assert.DeepEqual(t, <-recv, []byte("hello"))
}

// Old-stream frames are silently ignored on the client side when the local
// stream ID has advanced past the incoming frame's stream ID.
func TestManageReader_OldStreamFramesIgnored(t *testing.T) {
	ctx := drpctest.NewTracker(t)
	defer ctx.Close()

	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	cman := NewWithOptions(cconn, Options{SoftCancel: true})
	defer func() { _ = cman.Close() }()

	// Drain all client writes so nothing blocks, and write server
	// responses once we've seen enough data.
	ctx.Run(func(ctx context.Context) {
		buf := make([]byte, 4096)
		for {
			_, err := sconn.Read(buf)
			if err != nil {
				return
			}
		}
	})

	// Create stream 1 on the client, then cancel it so it finishes.
	subctx, cancel := context.WithCancel(ctx)
	stream1, err := cman.NewClientStream(subctx, "rpc1")
	assert.NoError(t, err)
	cancel()
	<-stream1.Finished()

	stream2, err := cman.NewClientStream(ctx, "rpc2")
	assert.NoError(t, err)

	// Write an old-stream frame (s1) then the real response for s2.
	// The s1 frame should be silently ignored by the client manager.
	writeFrames(t, sconn,
		createFrame(drpcwire.KindMessage, 1, 1, "old", true),
		createFrame(drpcwire.KindMessage, 2, 1, "new", true),
	)

	data, err := stream2.RawRecv()
	assert.NoError(t, err)
	assert.DeepEqual(t, data, []byte("new"))

	_ = stream2.Close()
}

// Non-invoke frames with no active stream are silently dropped, matching gRPC
// behavior. This covers both the case where a frame arrives before any stream
// is created and the case where a stream was removed.
func TestManageReader_NonInvokeWithNoStreamIgnored(t *testing.T) {
	for _, kind := range []drpcwire.Kind{
		drpcwire.KindMessage,
		drpcwire.KindCancel,
		drpcwire.KindClose,
		drpcwire.KindCloseSend,
		drpcwire.KindError,
	} {
		t.Run(kind.String(), func(t *testing.T) {
			ctx := drpctest.NewTracker(t)
			defer ctx.Close()

			cconn, sconn := net.Pipe()
			defer func() { _ = cconn.Close() }()
			defer func() { _ = sconn.Close() }()

			man := New(sconn)
			defer func() { _ = man.Close() }()

			// Send a non-invoke frame with no active stream.
			// It should be silently ignored.
			writeFrames(t, cconn,
				createFrame(kind, 1, 1, "", true),
			)

			// Follow up with a valid invoke. If the manager
			// terminated on the earlier frame, this would fail.
			recv := make(chan []byte, 1)
			ctx.Run(func(ctx context.Context) {
				stream, _, err := man.NewServerStream(ctx)
				assert.NoError(t, err)
				data, err := stream.RawRecv()
				assert.NoError(t, err)
				recv <- data
			})

			writeFrames(t, cconn,
				createFrame(drpcwire.KindInvoke, 2, 1, "rpc", true),
				createFrame(drpcwire.KindMessage, 2, 2, "hello", true),
			)

			assert.DeepEqual(t, <-recv, []byte("hello"))
		})
	}
}

// A valid invoke sequence: Invoke → Message.
// Metadata encoding is covered separately by TestDrpcMetadata.
func TestManageReader_ValidInvokeSequence(t *testing.T) {
	ctx := drpctest.NewTracker(t)
	defer ctx.Close()

	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	man := New(sconn)
	defer func() { _ = man.Close() }()

	recv := make(chan []byte, 1)
	ctx.Run(func(ctx context.Context) {
		stream, rpc, err := man.NewServerStream(ctx)
		assert.NoError(t, err)
		assert.Equal(t, rpc, "myrpc")

		data, err := stream.RawRecv()
		assert.NoError(t, err)
		recv <- data
	})

	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 1, 1, "myrpc", true),
		createFrame(drpcwire.KindMessage, 1, 2, "payload", true),
	)

	assert.DeepEqual(t, <-recv, []byte("payload"))
}

// Multi-frame message delivered through manager to stream: frames are
// assembled by the stream into a single packet.
func TestManageReader_MultiFrameDelivery(t *testing.T) {
	ctx := drpctest.NewTracker(t)
	defer ctx.Close()

	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	man := New(sconn)
	defer func() { _ = man.Close() }()

	recv := make(chan []byte, 1)
	ctx.Run(func(ctx context.Context) {
		stream, _, err := man.NewServerStream(ctx)
		assert.NoError(t, err)

		data, err := stream.RawRecv()
		assert.NoError(t, err)
		recv <- data
	})

	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 1, 1, "rpc", true),
		createFrame(drpcwire.KindMessage, 1, 2, "hel", false),
		createFrame(drpcwire.KindMessage, 1, 2, "lo ", false),
		createFrame(drpcwire.KindMessage, 1, 2, "world", true),
	)

	assert.DeepEqual(t, <-recv, []byte("hello world"))
}

// When a higher message ID arrives mid-assembly, the partial data is
// discarded and only the new message is delivered.
func TestManageReader_HigherMsgDiscardsInProgress(t *testing.T) {
	ctx := drpctest.NewTracker(t)
	defer ctx.Close()

	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	man := New(sconn)
	defer func() { _ = man.Close() }()

	recv := make(chan []byte, 1)
	ctx.Run(func(ctx context.Context) {
		stream, _, err := man.NewServerStream(ctx)
		assert.NoError(t, err)
		data, err := stream.RawRecv()
		assert.NoError(t, err)
		recv <- data
	})

	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 1, 1, "rpc", true),
		createFrame(drpcwire.KindMessage, 1, 2, "discard", false),
		createFrame(drpcwire.KindMessage, 1, 3, "kept", true),
	)

	assert.DeepEqual(t, <-recv, []byte("kept"))
}

// A continuation frame with a different kind than the first frame of the
// packet causes the manager to terminate with a protocol error.
func TestManageReader_KindChangeWithinPacket(t *testing.T) {
	ctx := drpctest.NewTracker(t)
	defer ctx.Close()

	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	man := New(sconn)
	defer func() { _ = man.Close() }()

	ctx.Run(func(ctx context.Context) {
		stream, _, err := man.NewServerStream(ctx)
		assert.NoError(t, err)
		for {
			if _, err := stream.RawRecv(); err != nil {
				return
			}
		}
	})

	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 1, 1, "rpc", true),
		createFrame(drpcwire.KindMessage, 1, 2, "data", false),
		createFrame(drpcwire.KindClose, 1, 2, "", true),
	)

	waitForClosed(t, man)
}

// Multi-frame assembly works correctly when the message ID is greater than
// the previous message (e.g., on the server side where invoke consumed
// earlier IDs).
func TestManageReader_MultiFrameWithSkippedMessageID(t *testing.T) {
	ctx := drpctest.NewTracker(t)
	defer ctx.Close()

	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	man := New(sconn)
	defer func() { _ = man.Close() }()

	recv := make(chan []byte, 1)
	ctx.Run(func(ctx context.Context) {
		stream, _, err := man.NewServerStream(ctx)
		assert.NoError(t, err)
		data, err := stream.RawRecv()
		assert.NoError(t, err)
		recv <- data
	})

	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 1, 1, "rpc", true),
		createFrame(drpcwire.KindMessage, 1, 3, "hel", false),
		createFrame(drpcwire.KindMessage, 1, 3, "lo", true),
	)

	assert.DeepEqual(t, <-recv, []byte("hello"))
}

// A second invoke for the same stream ID is rejected — the stream treats
// it as a protocol error, terminating the manager.
func TestManageReader_InvokeOnExistingStream(t *testing.T) {
	ctx := drpctest.NewTracker(t)
	defer ctx.Close()

	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	man := New(sconn)
	defer func() { _ = man.Close() }()

	ctx.Run(func(ctx context.Context) {
		stream, _, err := man.NewServerStream(ctx)
		assert.NoError(t, err)
		_ = stream
	})

	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 1, 1, "rpc1", true),
		createFrame(drpcwire.KindInvoke, 1, 2, "rpc2", true),
	)

	waitForClosed(t, man)
	assert.That(t, drpc.ProtocolError.Has(man.sigs.term.Err()))
}

// When a non-invoke frame arrives before the stream is created (e.g.,
// NewServerStream hasn't returned yet), manageReader waits for the stream
// and retries.
func TestManageReader_WaitsForStreamCreation(t *testing.T) {
	ctx := drpctest.NewTracker(t)
	defer ctx.Close()

	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	man := New(sconn)
	defer func() { _ = man.Close() }()

	// Write invoke + message immediately. The message arrives before
	// NewServerStream creates the stream, exercising the default/wait path.
	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 1, 1, "rpc", true),
		createFrame(drpcwire.KindMessage, 1, 2, "data", true),
	)

	// Small delay to let manageReader process both frames.
	time.Sleep(10 * time.Millisecond)

	recv := make(chan []byte, 1)
	ctx.Run(func(ctx context.Context) {
		stream, _, err := man.NewServerStream(ctx)
		assert.NoError(t, err)

		data, err := stream.RawRecv()
		assert.NoError(t, err)
		recv <- data
	})

	assert.DeepEqual(t, <-recv, []byte("data"))
}

// When a server stream's context is canceled, manageStream removes the stream
// from the active registry. Any in-flight frames for that stream that arrive
// after removal should be silently dropped, not terminate the connection. This
// reproduces the scenario that caused flaky ambiguous-result errors in
// CockroachDB when DRPC was enabled.
func TestManageReader_LateFrameAfterStreamRemoved(t *testing.T) {
	ctx := drpctest.NewTracker(t)
	defer ctx.Close()

	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	sman := NewWithOptions(sconn, Options{SoftCancel: true})
	defer func() { _ = sman.Close() }()

	// Drain client-side writes so the server manager doesn't block.
	ctx.Run(func(ctx context.Context) {
		buf := make([]byte, 4096)
		for {
			if _, err := cconn.Read(buf); err != nil {
				return
			}
		}
	})

	// Send invoke for stream 1 and create a server stream with a
	// cancelable context.
	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 1, 1, "rpc1", true),
	)

	subctx, cancel := context.WithCancel(ctx)
	stream1, _, err := sman.NewServerStream(subctx)
	assert.NoError(t, err)

	// Cancel the server stream's context. This triggers manageStream to
	// call active.Remove(), leaving no active stream in the registry.
	cancel()

	// Wait for the stream to be fully removed from the registry.
	<-stream1.Finished()

	// Send a late non-invoke frame for stream 1. With the old (broken)
	// behavior this would terminate the connection. Now it should be
	// silently dropped.
	writeFrames(t, cconn,
		createFrame(drpcwire.KindCloseSend, 1, 2, "", true),
	)

	// Verify the manager is still alive by successfully creating a new
	// stream for a subsequent invoke.
	recv := make(chan []byte, 1)
	ctx.Run(func(ctx context.Context) {
		stream2, _, err := sman.NewServerStream(ctx)
		assert.NoError(t, err)
		data, err := stream2.RawRecv()
		assert.NoError(t, err)
		recv <- data
	})

	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 2, 1, "rpc2", true),
		createFrame(drpcwire.KindMessage, 2, 2, "alive", true),
	)

	assert.DeepEqual(t, <-recv, []byte("alive"))
}

type blockingTransport chan struct{}

func (b blockingTransport) Read(p []byte) (n int, err error)  { <-b; return 0, io.EOF }
func (b blockingTransport) Write(p []byte) (n int, err error) { <-b; return 0, io.EOF }
func (b blockingTransport) Close() error                      { close(b); return nil }

// Unblocked always returns a closed channel with multiplexing support.
func TestUnblocked_AlwaysReady(t *testing.T) {
	man := New(make(blockingTransport))
	defer func() { _ = man.Close() }()

	assert.That(t, closed(man.Unblocked()))
}
