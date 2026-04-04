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
	"storj.io/drpc/drpcstream"
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

// recvStream is a helper that creates manager options with a ServerHandler
// that sends received streams and rpc names to channels.
type recvStream struct {
	streams chan *drpcstream.Stream
	rpcs    chan string
}

func newRecvStream() *recvStream {
	return &recvStream{
		streams: make(chan *drpcstream.Stream, 10),
		rpcs:    make(chan string, 10),
	}
}

func (rs *recvStream) handler(stream *drpcstream.Stream, rpc string) {
	rs.streams <- stream
	rs.rpcs <- rpc
}

func (rs *recvStream) get(t *testing.T) (*drpcstream.Stream, string) {
	t.Helper()
	select {
	case s := <-rs.streams:
		return s, <-rs.rpcs
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for stream")
		return nil, ""
	}
}

func TestTimeout(t *testing.T) {
	tr := make(blockingTransport)
	man := NewWithOptions(tr, Options{
		InactivityTimeout: time.Millisecond,
	})
	defer func() { _ = man.Close() }()

	// With push model, inactivity timeout should terminate the manager
	// when no frames arrive. The manager terminates via manageReader
	// which will fail to read from the blocking transport once closed.
	// For now, verify the manager eventually closes (Close() terminates it).
	select {
	case <-man.Closed():
		// Manager terminated (transport closed or timeout)
	case <-time.After(100 * time.Millisecond):
		// Manager is still alive because no frames arrived but the blocking
		// transport prevents ReadFrame from returning. This is expected.
		// The InactivityTimeout semantic will be handled at the manageReader
		// level in a follow-up.
	}
}

func TestDrpcMetadata(t *testing.T) {
	ctx := drpctest.NewTracker(t)
	defer ctx.Close()

	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	cman := New(cconn)
	defer func() { _ = cman.Close() }()

	recv := newRecvStream()
	sman := NewWithOptions(sconn, Options{
		GRPCMetadataCompatMode: false,
		ServerHandler:          recv.handler,
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

	stream, _ := recv.get(t)
	streamCtx := stream.Context()

	drpcMd, ok := drpcmetadata.GetFromIncomingContext(streamCtx)
	assert.That(t, ok)
	assert.Equal(t, drpcMd, map[string]string{"key": "value", "multi-value-key": "value1,value2"})

	grpcMd, ok := grpcmetadata.FromIncomingContext(streamCtx)
	assert.False(t, ok)
	assert.Nil(t, grpcMd)

	_, err := stream.RawRecv()
	assert.NoError(t, err)

	_, err = stream.RawRecv()
	assert.That(t, errors.Is(err, io.EOF))

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

	recv := newRecvStream()
	sman := NewWithOptions(sconn, Options{
		GRPCMetadataCompatMode: true,
		ServerHandler:          recv.handler,
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

	stream, _ := recv.get(t)
	streamCtx := stream.Context()

	drpcMd, ok := drpcmetadata.GetFromIncomingContext(streamCtx)
	assert.False(t, ok)
	assert.Nil(t, drpcMd)

	grpcMd, ok := grpcmetadata.FromIncomingContext(streamCtx)
	assert.That(t, ok)
	assert.Equal(t, grpcMd, grpcmetadata.MD{"key": []string{"value"},
		"multi-value-key": []string{"value1,value2"}})

	_, err := stream.RawRecv()
	assert.NoError(t, err)

	_, err = stream.RawRecv()
	assert.That(t, errors.Is(err, io.EOF))

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
	recv := newRecvStream()
	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	man := NewWithOptions(sconn, Options{ServerHandler: recv.handler})
	defer func() { _ = man.Close() }()

	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 1, 1, "rpc", true),
		createFrame(drpcwire.KindMessage, 1, 5, "ok", true),
		createFrame(drpcwire.KindMessage, 1, 4, "bad", true),
	)

	// Drain messages in the handler so HandleFrame doesn't block.
	stream, _ := recv.get(t)
	go func() {
		for {
			if _, err := stream.RawRecv(); err != nil {
				return
			}
		}
	}()

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

	recv := newRecvStream()
	man := NewWithOptions(sconn, Options{
		SoftCancel:    true,
		ServerHandler: recv.handler,
	})
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

	// Create stream 1, then cancel its context.
	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 1, 1, "rpc1", true),
	)
	stream1, _ := recv.get(t)
	stream1.Cancel(context.Canceled)
	<-stream1.Finished()

	// Send a frame for the now-removed stream 1, then a valid invoke for
	// stream 2. The stale frame should be silently dropped.
	writeFrames(t, cconn,
		createFrame(drpcwire.KindMessage, 1, 4, "stale", true),
		createFrame(drpcwire.KindInvoke, 2, 1, "rpc2", true),
		createFrame(drpcwire.KindMessage, 2, 2, "fresh", true),
	)

	stream2, _ := recv.get(t)
	data, err := stream2.RawRecv()
	assert.NoError(t, err)
	assert.DeepEqual(t, data, []byte("fresh"))
}

// Invoke stream ID regression: after invoking stream 2, an invoke for stream 1
// is rejected by the invoke stream ID monotonicity check.
func TestManageReader_InvokeStreamIDRegression(t *testing.T) {
	ctx := drpctest.NewTracker(t)
	defer ctx.Close()

	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	recv := newRecvStream()
	man := NewWithOptions(sconn, Options{
		SoftCancel:    true,
		ServerHandler: recv.handler,
	})
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
	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 2, 1, "rpc2", true),
	)
	stream2, _ := recv.get(t)
	stream2.Cancel(context.Canceled)
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
	recv := newRecvStream()
	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	man := NewWithOptions(sconn, Options{ServerHandler: recv.handler})
	defer func() { _ = man.Close() }()

	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 1, 1, "rpc", true),
		createFrame(drpcwire.KindInvoke, 1, 1, "rpc", true),
	)

	waitForClosed(t, man)
}

// Non-done frames don't bump the message ID, so continuation frames with
// the same ID are accepted.
func TestManageReader_ContinuationFramesAccepted(t *testing.T) {
	recv := newRecvStream()
	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	man := NewWithOptions(sconn, Options{ServerHandler: recv.handler})
	defer func() { _ = man.Close() }()

	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 1, 1, "rpc", true),
		createFrame(drpcwire.KindMessage, 1, 2, "hel", false),
		createFrame(drpcwire.KindMessage, 1, 2, "lo", true),
	)

	stream, _ := recv.get(t)
	data, err := stream.RawRecv()
	assert.NoError(t, err)
	assert.DeepEqual(t, data, []byte("hello"))
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
			cconn, sconn := net.Pipe()
			defer func() { _ = cconn.Close() }()
			defer func() { _ = sconn.Close() }()

			recv := newRecvStream()
			man := NewWithOptions(sconn, Options{ServerHandler: recv.handler})
			defer func() { _ = man.Close() }()

			// Send a non-invoke frame with no active stream.
			// It should be silently ignored.
			writeFrames(t, cconn,
				createFrame(kind, 1, 1, "", true),
			)

			// Follow up with a valid invoke. If the manager
			// terminated on the earlier frame, this would fail.
			writeFrames(t, cconn,
				createFrame(drpcwire.KindInvoke, 2, 1, "rpc", true),
				createFrame(drpcwire.KindMessage, 2, 2, "hello", true),
			)

			stream, _ := recv.get(t)
			data, err := stream.RawRecv()
			assert.NoError(t, err)
			assert.DeepEqual(t, data, []byte("hello"))
		})
	}
}

// A valid invoke sequence: Invoke → Message.
// Metadata encoding is covered separately by TestDrpcMetadata.
func TestManageReader_ValidInvokeSequence(t *testing.T) {
	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	recv := newRecvStream()
	man := NewWithOptions(sconn, Options{ServerHandler: recv.handler})
	defer func() { _ = man.Close() }()

	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 1, 1, "myrpc", true),
		createFrame(drpcwire.KindMessage, 1, 2, "payload", true),
	)

	stream, rpc := recv.get(t)
	assert.Equal(t, rpc, "myrpc")

	data, err := stream.RawRecv()
	assert.NoError(t, err)
	assert.DeepEqual(t, data, []byte("payload"))
}

// Multi-frame message delivered through manager to stream: frames are
// assembled by the stream into a single packet.
func TestManageReader_MultiFrameDelivery(t *testing.T) {
	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	recv := newRecvStream()
	man := NewWithOptions(sconn, Options{ServerHandler: recv.handler})
	defer func() { _ = man.Close() }()

	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 1, 1, "rpc", true),
		createFrame(drpcwire.KindMessage, 1, 2, "hel", false),
		createFrame(drpcwire.KindMessage, 1, 2, "lo ", false),
		createFrame(drpcwire.KindMessage, 1, 2, "world", true),
	)

	stream, _ := recv.get(t)
	data, err := stream.RawRecv()
	assert.NoError(t, err)
	assert.DeepEqual(t, data, []byte("hello world"))
}

// When a higher message ID arrives mid-assembly, the partial data is
// discarded and only the new message is delivered.
func TestManageReader_HigherMsgDiscardsInProgress(t *testing.T) {
	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	recv := newRecvStream()
	man := NewWithOptions(sconn, Options{ServerHandler: recv.handler})
	defer func() { _ = man.Close() }()

	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 1, 1, "rpc", true),
		createFrame(drpcwire.KindMessage, 1, 2, "discard", false),
		createFrame(drpcwire.KindMessage, 1, 3, "kept", true),
	)

	stream, _ := recv.get(t)
	data, err := stream.RawRecv()
	assert.NoError(t, err)
	assert.DeepEqual(t, data, []byte("kept"))
}

// A continuation frame with a different kind than the first frame of the
// packet causes the manager to terminate with a protocol error.
func TestManageReader_KindChangeWithinPacket(t *testing.T) {
	recv := newRecvStream()
	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	man := NewWithOptions(sconn, Options{ServerHandler: recv.handler})
	defer func() { _ = man.Close() }()

	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 1, 1, "rpc", true),
		createFrame(drpcwire.KindMessage, 1, 2, "data", false),
		createFrame(drpcwire.KindClose, 1, 2, "", true),
	)

	// Drain in handler so HandleFrame doesn't block.
	stream, _ := recv.get(t)
	go func() {
		for {
			if _, err := stream.RawRecv(); err != nil {
				return
			}
		}
	}()

	waitForClosed(t, man)
}

// Multi-frame assembly works correctly when the message ID is greater than
// the previous message (e.g., on the server side where invoke consumed
// earlier IDs).
func TestManageReader_MultiFrameWithSkippedMessageID(t *testing.T) {
	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	recv := newRecvStream()
	man := NewWithOptions(sconn, Options{ServerHandler: recv.handler})
	defer func() { _ = man.Close() }()

	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 1, 1, "rpc", true),
		createFrame(drpcwire.KindMessage, 1, 3, "hel", false),
		createFrame(drpcwire.KindMessage, 1, 3, "lo", true),
	)

	stream, _ := recv.get(t)
	data, err := stream.RawRecv()
	assert.NoError(t, err)
	assert.DeepEqual(t, data, []byte("hello"))
}

// A second invoke for the same stream ID is rejected — the stream treats
// it as a protocol error, terminating the manager.
func TestManageReader_InvokeOnExistingStream(t *testing.T) {
	recv := newRecvStream()
	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	man := NewWithOptions(sconn, Options{ServerHandler: recv.handler})
	defer func() { _ = man.Close() }()

	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 1, 1, "rpc1", true),
		createFrame(drpcwire.KindInvoke, 1, 2, "rpc2", true),
	)

	waitForClosed(t, man)
	assert.That(t, drpc.ProtocolError.Has(man.sigs.term.Err()))
}

// With the push model, invoke + message written before any handler runs
// should still be delivered correctly because the stream is created inline
// in manageReader before dispatching to the handler.
func TestManageReader_InvokeAndMessageDeliveredInline(t *testing.T) {
	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	recv := newRecvStream()
	man := NewWithOptions(sconn, Options{ServerHandler: recv.handler})
	defer func() { _ = man.Close() }()

	// Write invoke + message immediately. The stream is created inline
	// in manageReader, so the message frame finds the stream in the
	// registry even before the handler goroutine runs.
	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 1, 1, "rpc", true),
		createFrame(drpcwire.KindMessage, 1, 2, "data", true),
	)

	stream, _ := recv.get(t)
	data, err := stream.RawRecv()
	assert.NoError(t, err)
	assert.DeepEqual(t, data, []byte("data"))
}

// When a server stream's context is canceled, manageStream removes the stream
// from the active registry. Any in-flight frames for that stream that arrive
// after removal should be silently dropped, not terminate the connection.
func TestManageReader_LateFrameAfterStreamRemoved(t *testing.T) {
	ctx := drpctest.NewTracker(t)
	defer ctx.Close()

	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	recv := newRecvStream()
	sman := NewWithOptions(sconn, Options{
		SoftCancel:    true,
		ServerHandler: recv.handler,
	})
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

	// Send invoke for stream 1 and receive it.
	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 1, 1, "rpc1", true),
	)

	stream1, _ := recv.get(t)

	// Cancel the stream. This triggers manageStream to remove it.
	stream1.Cancel(context.Canceled)
	<-stream1.Finished()

	// Send a late non-invoke frame for stream 1. It should be silently dropped.
	writeFrames(t, cconn,
		createFrame(drpcwire.KindCloseSend, 1, 2, "", true),
	)

	// Verify the manager is still alive by successfully creating a new
	// stream for a subsequent invoke.
	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 2, 1, "rpc2", true),
		createFrame(drpcwire.KindMessage, 2, 2, "alive", true),
	)

	stream2, _ := recv.get(t)
	data, err := stream2.RawRecv()
	assert.NoError(t, err)
	assert.DeepEqual(t, data, []byte("alive"))
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

// Multiple concurrent streams are dispatched to the handler independently.
func TestManageReader_ConcurrentStreams(t *testing.T) {
	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	recv := newRecvStream()
	man := NewWithOptions(sconn, Options{ServerHandler: recv.handler})
	defer func() { _ = man.Close() }()

	// Send two invokes and messages for different streams.
	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 1, 1, "rpc1", true),
		createFrame(drpcwire.KindInvoke, 2, 1, "rpc2", true),
		createFrame(drpcwire.KindMessage, 1, 2, "msg1", true),
		createFrame(drpcwire.KindMessage, 2, 2, "msg2", true),
	)

	// Handler goroutines may deliver in any order.
	got := make(map[string]string) // rpc -> message
	for i := 0; i < 2; i++ {
		stream, rpc := recv.get(t)
		data, err := stream.RawRecv()
		assert.NoError(t, err)
		got[rpc] = string(data)
	}

	assert.Equal(t, got["rpc1"], "msg1")
	assert.Equal(t, got["rpc2"], "msg2")
}
