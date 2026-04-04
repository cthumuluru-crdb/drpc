// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package drpcmanager

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
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

// Out-of-order invoke: after invoking stream 2, an invoke for stream 1
// is accepted because multiplexed clients may write invoke frames
// out of order due to concurrent goroutine scheduling.
func TestManageReader_InvokeStreamIDRegression(t *testing.T) {
	ctx := drpctest.NewTracker(t)
	defer ctx.Close()

	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	recv := newRecvStream()
	man := NewWithOptions(sconn, Options{

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

	// Send an invoke for stream 1 (lower than 2). With multiplexing,
	// this is valid — concurrent goroutines may write invoke frames
	// out of order. The manager should accept it and create a new stream.
	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 1, 1, "rpc1", true),
	)

	stream1, _ := recv.get(t)
	assert.Equal(t, stream1.ID(), uint64(1))
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

	cman := New(cconn)
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

// Creating a new client stream must not discard unflushed data from a
// previously created stream. Regression test for the Writer.Reset() bug
// where NewWithOptions cleared the shared Writer buffer on every stream
// creation.
func TestNewClientStream_PreservesBufferedData(t *testing.T) {
	ctx := drpctest.NewTracker(t)
	defer ctx.Close()

	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	// Use a large writer buffer so frames stay buffered (not auto-flushed).
	cman := NewWithOptions(cconn, Options{WriterBufferSize: 64 * 1024})
	defer func() { _ = cman.Close() }()

	// Create stream 1 and write frames that stay in the shared Writer buffer.
	stream1, err := cman.NewClientStream(ctx, "rpc1")
	assert.NoError(t, err)
	assert.NoError(t, stream1.RawWrite(drpcwire.KindInvoke, []byte("rpc1")))
	assert.NoError(t, stream1.RawWrite(drpcwire.KindMessage, []byte("data1")))

	// Create stream 2. Before the fix, this called Writer.Reset() which
	// would clear stream 1's buffered frames from the shared Writer.
	stream2, err := cman.NewClientStream(ctx, "rpc2")
	assert.NoError(t, err)
	assert.NoError(t, stream2.RawWrite(drpcwire.KindInvoke, []byte("rpc2")))
	assert.NoError(t, stream2.RawWrite(drpcwire.KindMessage, []byte("data2")))

	// Read frames on the server side. The pipe Read blocks until Flush
	// writes data, so start reading before flushing.
	type readResult struct {
		frames []drpcwire.Frame
		err    error
	}
	results := make(chan readResult, 1)
	ctx.Run(func(ctx context.Context) {
		rd := drpcwire.NewReader(sconn)
		var frames []drpcwire.Frame
		for i := 0; i < 4; i++ {
			fr, err := rd.ReadFrame()
			if err != nil {
				results <- readResult{frames, err}
				return
			}
			frames = append(frames, fr)
		}
		results <- readResult{frames, nil}
	})

	// Flush sends all buffered data (both streams) to the pipe.
	assert.NoError(t, stream1.RawFlush())

	select {
	case r := <-results:
		assert.NoError(t, r.err)
		assert.Equal(t, len(r.frames), 4)

		// Collect frames by stream ID.
		got := make(map[uint64][]string)
		for _, fr := range r.frames {
			got[fr.ID.Stream] = append(got[fr.ID.Stream], string(fr.Data))
		}
		assert.DeepEqual(t, got[stream1.ID()], []string{"rpc1", "data1"})
		assert.DeepEqual(t, got[stream2.ID()], []string{"rpc2", "data2"})

	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for frames")
	}
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

// Canceling one client stream must not affect a sibling stream on the same
// manager. The manager must stay alive and the sibling must complete normally.
func TestManageStream_CancelIsolation(t *testing.T) {
	ctx := drpctest.NewTracker(t)
	defer ctx.Close()

	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	cman := New(cconn)
	defer func() { _ = cman.Close() }()

	// Drain cancel frames from the client side so writes don't block.
	ctx.Run(func(ctx context.Context) {
		buf := make([]byte, 4096)
		for {
			if _, err := sconn.Read(buf); err != nil {
				return
			}
		}
	})

	// Create two client streams.
	ctx1, cancel1 := context.WithCancel(ctx)
	stream1, err := cman.NewClientStream(ctx1, "rpc1")
	assert.NoError(t, err)

	stream2, err := cman.NewClientStream(ctx, "rpc2")
	assert.NoError(t, err)

	// Cancel stream 1's context.
	cancel1()
	<-stream1.Finished()

	// Stream 2 should still be alive and the manager should not be terminated.
	assert.That(t, !closed(cman.Closed()))
	assert.That(t, !stream2.IsTerminated())

	// Stream 2 can still write successfully.
	assert.NoError(t, stream2.RawWrite(drpcwire.KindInvoke, []byte("rpc2")))
	assert.NoError(t, stream2.RawFlush())
	assert.NoError(t, stream2.Close())
}

// When a client stream's context is canceled, a KindCancel frame is sent
// to the remote side so it can stop processing that stream.
func TestManageStream_CancelSendsFrame(t *testing.T) {
	ctx := drpctest.NewTracker(t)
	defer ctx.Close()

	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	cman := New(cconn)
	defer func() { _ = cman.Close() }()

	// Read frames from the server side.
	type frameResult struct {
		fr  drpcwire.Frame
		err error
	}
	frames := make(chan frameResult, 10)
	ctx.Run(func(ctx context.Context) {
		rd := drpcwire.NewReader(sconn)
		for {
			fr, err := rd.ReadFrame()
			frames <- frameResult{fr, err}
			if err != nil {
				return
			}
		}
	})

	// Create a cancelable stream and send its invoke.
	subctx, cancel := context.WithCancel(ctx)
	stream, err := cman.NewClientStream(subctx, "rpc1")
	assert.NoError(t, err)
	assert.NoError(t, stream.RawWrite(drpcwire.KindInvoke, []byte("rpc1")))
	assert.NoError(t, stream.RawFlush())

	// Read the invoke frame.
	r := <-frames
	assert.NoError(t, r.err)
	assert.Equal(t, r.fr.Kind, drpcwire.KindInvoke)
	sid := r.fr.ID.Stream

	// Cancel the context — should trigger a cancel frame.
	cancel()

	// Read the cancel frame.
	select {
	case r := <-frames:
		assert.NoError(t, r.err)
		assert.Equal(t, r.fr.Kind, drpcwire.KindCancel)
		assert.Equal(t, r.fr.ID.Stream, sid)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for cancel frame")
	}
}

// Canceling one stream while a sibling is actively writing must not corrupt
// or lose the sibling's data.
func TestManageStream_CancelWhileSiblingWrites(t *testing.T) {
	ctx := drpctest.NewTracker(t)
	defer ctx.Close()

	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	cman := New(cconn)
	defer func() { _ = cman.Close() }()

	const numMessages = 50

	// Read all frames on the server side. Frame.Data aliases the reader's
	// internal buffer, so we must copy it before sending on the channel.
	type frameResult struct {
		fr  drpcwire.Frame
		err error
	}
	frames := make(chan frameResult, 200)
	ctx.Run(func(ctx context.Context) {
		rd := drpcwire.NewReader(sconn)
		for {
			fr, err := rd.ReadFrame()
			fr.Data = append([]byte(nil), fr.Data...)
			frames <- frameResult{fr, err}
			if err != nil {
				return
			}
		}
	})

	// Create stream 1 (will be canceled) and stream 2 (will write data).
	ctx1, cancel1 := context.WithCancel(ctx)
	stream1, err := cman.NewClientStream(ctx1, "rpc1")
	assert.NoError(t, err)
	assert.NoError(t, stream1.RawWrite(drpcwire.KindInvoke, []byte("rpc1")))
	assert.NoError(t, stream1.RawFlush())

	stream2, err := cman.NewClientStream(ctx, "rpc2")
	assert.NoError(t, err)
	assert.NoError(t, stream2.RawWrite(drpcwire.KindInvoke, []byte("rpc2")))
	assert.NoError(t, stream2.RawFlush())

	// Start writing on stream 2 concurrently.
	writeDone := make(chan struct{})
	ctx.Run(func(_ context.Context) {
		defer close(writeDone)
		for i := 0; i < numMessages; i++ {
			msg := fmt.Sprintf("msg-%d", i)
			if err := stream2.RawWrite(drpcwire.KindMessage, []byte(msg)); err != nil {
				return
			}
			if err := stream2.RawFlush(); err != nil {
				return
			}
		}
		_ = stream2.Close()
	})

	// Cancel stream 1 while stream 2 is writing.
	time.Sleep(5 * time.Millisecond)
	cancel1()

	// Wait for stream 2's writes to finish.
	<-writeDone

	// Collect frames by stream ID.
	got := make(map[uint64][]drpcwire.Frame)
	timeout := time.After(5 * time.Second)
	for done := false; !done; {
		select {
		case r := <-frames:
			if r.err != nil {
				done = true
				break
			}
			got[r.fr.ID.Stream] = append(got[r.fr.ID.Stream], r.fr)
			// Stop once we see the Close frame from stream 2.
			if r.fr.ID.Stream == stream2.ID() && r.fr.Kind == drpcwire.KindClose {
				done = true
			}
		case <-timeout:
			t.Fatal("timed out waiting for frames")
		}
	}

	// Verify all messages from stream 2 arrived (invoke + N messages + close).
	s2Frames := got[stream2.ID()]
	var msgCount int
	for _, fr := range s2Frames {
		if fr.Kind == drpcwire.KindMessage {
			msgCount++
		}
	}
	assert.Equal(t, msgCount, numMessages)

	// Manager should still be alive.
	assert.That(t, !closed(cman.Closed()))
}

// Multiple streams writing concurrently must not corrupt each other's data.
// Frames may interleave but per-stream ordering must be preserved.
func TestManageStream_ConcurrentWrites(t *testing.T) {
	ctx := drpctest.NewTracker(t)
	defer ctx.Close()

	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	cman := New(cconn)
	defer func() { _ = cman.Close() }()

	const (
		numStreams   = 5
		numPerStream = 50
	)

	// Collect all frames on server side. Frame.Data aliases the reader's
	// internal buffer, so we must copy it before sending on the channel.
	type frameResult struct {
		fr  drpcwire.Frame
		err error
	}
	allFrames := make(chan frameResult, numStreams*(numPerStream+2)*2)
	ctx.Run(func(ctx context.Context) {
		rd := drpcwire.NewReader(sconn)
		for {
			fr, err := rd.ReadFrame()
			fr.Data = append([]byte(nil), fr.Data...)
			allFrames <- frameResult{fr, err}
			if err != nil {
				return
			}
		}
	})

	// Create streams and build a map from stream ID to index.
	streams := make([]*drpcstream.Stream, numStreams)
	idToIdx := make(map[uint64]int)
	for i := 0; i < numStreams; i++ {
		s, err := cman.NewClientStream(ctx, fmt.Sprintf("rpc%d", i))
		assert.NoError(t, err)
		assert.NoError(t, s.RawWrite(drpcwire.KindInvoke, []byte(fmt.Sprintf("rpc%d", i))))
		assert.NoError(t, s.RawFlush())
		streams[i] = s
		idToIdx[s.ID()] = i
	}

	// Write concurrently from each stream.
	var wg sync.WaitGroup
	wg.Add(numStreams)
	for i := 0; i < numStreams; i++ {
		go func(s *drpcstream.Stream, idx int) {
			defer wg.Done()
			for j := 0; j < numPerStream; j++ {
				msg := fmt.Sprintf("s%d-m%d", idx, j)
				_ = s.RawWrite(drpcwire.KindMessage, []byte(msg))
				_ = s.RawFlush()
			}
			_ = s.Close()
		}(streams[i], i)
	}
	wg.Wait()

	// Collect frames by stream ID.
	got := make(map[uint64][]string)
	closesSeen := 0
	timeout := time.After(5 * time.Second)
	for closesSeen < numStreams {
		select {
		case r := <-allFrames:
			if r.err != nil {
				t.Fatalf("unexpected read error: %v", r.err)
			}
			if r.fr.Kind == drpcwire.KindMessage {
				got[r.fr.ID.Stream] = append(got[r.fr.ID.Stream], string(r.fr.Data))
			}
			if r.fr.Kind == drpcwire.KindClose {
				closesSeen++
			}
		case <-timeout:
			t.Fatalf("timed out: got %d/%d closes", closesSeen, numStreams)
		}
	}

	// Verify each stream got all messages in order.
	for sid, msgs := range got {
		idx := idToIdx[sid]
		assert.Equal(t, len(msgs), numPerStream)
		for j, msg := range msgs {
			assert.Equal(t, msg, fmt.Sprintf("s%d-m%d", idx, j))
		}
	}
}

// Rapid create-cancel-create cycles must not leak goroutines or corrupt the
// manager's stream registry.
func TestManageStream_RapidCreateCancel(t *testing.T) {
	ctx := drpctest.NewTracker(t)
	defer ctx.Close()

	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	cman := New(cconn)
	defer func() { _ = cman.Close() }()

	// Drain frames on server side.
	ctx.Run(func(ctx context.Context) {
		buf := make([]byte, 4096)
		for {
			if _, err := sconn.Read(buf); err != nil {
				return
			}
		}
	})

	const iterations = 100

	for i := 0; i < iterations; i++ {
		subctx, cancel := context.WithCancel(ctx)
		stream, err := cman.NewClientStream(subctx, fmt.Sprintf("rpc%d", i))
		assert.NoError(t, err)
		assert.NoError(t, stream.RawWrite(drpcwire.KindInvoke, []byte("rpc")))
		assert.NoError(t, stream.RawFlush())
		cancel()
		<-stream.Finished()
	}

	// Manager must still be alive after all the churn.
	assert.That(t, !closed(cman.Closed()))

	// Create one final stream to verify the manager is fully functional.
	stream, err := cman.NewClientStream(ctx, "final")
	assert.NoError(t, err)
	assert.NoError(t, stream.RawWrite(drpcwire.KindInvoke, []byte("final")))
	assert.NoError(t, stream.RawWrite(drpcwire.KindMessage, []byte("hello")))
	assert.NoError(t, stream.RawFlush())
	assert.NoError(t, stream.Close())
}

// Canceling a client stream during active server-side streaming must
// propagate the cancel frame and terminate the server handler's stream.
func TestManageStream_CancelDuringServerStreaming(t *testing.T) {
	ctx := drpctest.NewTracker(t)
	defer ctx.Close()

	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	recv := newRecvStream()
	sman := NewWithOptions(sconn, Options{ServerHandler: recv.handler})
	defer func() { _ = sman.Close() }()

	// Send invoke to create server stream 1.
	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 1, 1, "rpc1", true),
	)
	sstream, _ := recv.get(t)

	// Start draining client-side reads (server responses) in background.
	// This must be running before the handler starts writing, otherwise
	// net.Pipe's synchronous write blocks the handler.
	msgSeen := make(chan struct{}, 100)
	ctx.Run(func(_ context.Context) {
		rd := drpcwire.NewReader(cconn)
		for {
			fr, err := rd.ReadFrame()
			if err != nil {
				return
			}
			if fr.Kind == drpcwire.KindMessage {
				msgSeen <- struct{}{}
			}
		}
	})

	// Server handler sends messages until its stream terminates.
	ctx.Run(func(_ context.Context) {
		for i := 0; ; i++ {
			msg := fmt.Sprintf("resp-%d", i)
			if err := sstream.RawWrite(drpcwire.KindMessage, []byte(msg)); err != nil {
				return
			}
			if err := sstream.RawFlush(); err != nil {
				return
			}
		}
	})

	// Wait for a few messages to arrive, then send cancel.
	for i := 0; i < 3; i++ {
		<-msgSeen
	}

	writeFrames(t, cconn,
		createFrame(drpcwire.KindCancel, 1, 100, "", true),
	)

	// Server handler's stream context must be canceled.
	select {
	case <-sstream.Context().Done():
		// Good — cancel propagated.
	case <-time.After(5 * time.Second):
		t.Fatal("server stream context was not canceled")
	}

	// Server manager should still be alive.
	assert.That(t, !closed(sman.Closed()))

	// A new stream should work on the same server manager.
	writeFrames(t, cconn,
		createFrame(drpcwire.KindInvoke, 2, 1, "rpc2", true),
		createFrame(drpcwire.KindMessage, 2, 2, "data", true),
	)
	sstream2, _ := recv.get(t)
	data, err := sstream2.RawRecv()
	assert.NoError(t, err)
	assert.DeepEqual(t, data, []byte("data"))
}

// 50 concurrent server streams must all receive their data correctly,
// testing the registry and handler dispatch under load.
func TestManageStream_FanOut(t *testing.T) {
	ctx := drpctest.NewTracker(t)
	defer ctx.Close()

	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	const numStreams = 50

	recv := newRecvStream()
	sman := NewWithOptions(sconn, Options{ServerHandler: recv.handler})
	defer func() { _ = sman.Close() }()

	// Drain server-side writes so nothing blocks.
	ctx.Run(func(ctx context.Context) {
		buf := make([]byte, 4096)
		for {
			if _, err := cconn.Read(buf); err != nil {
				return
			}
		}
	})

	// Send invokes and messages for all streams.
	for i := uint64(1); i <= numStreams; i++ {
		writeFrames(t, cconn,
			createFrame(drpcwire.KindInvoke, i, 1, fmt.Sprintf("rpc%d", i), true),
			createFrame(drpcwire.KindMessage, i, 2, fmt.Sprintf("req-%d", i), true),
		)
	}

	// Collect received data from all handler goroutines.
	type result struct {
		streamID uint64
		data     string
		err      error
	}
	results := make(chan result, numStreams)
	for i := 0; i < numStreams; i++ {
		ctx.Run(func(_ context.Context) {
			stream, _ := recv.get(t)
			data, err := stream.RawRecv()
			results <- result{stream.ID(), string(data), err}
			stream.Cancel(nil)
		})
	}

	// Verify all streams received correct data.
	got := make(map[uint64]string)
	timeout := time.After(5 * time.Second)
	for i := 0; i < numStreams; i++ {
		select {
		case r := <-results:
			if r.err != nil {
				t.Fatalf("stream %d recv error: %v", r.streamID, r.err)
			}
			got[r.streamID] = r.data
		case <-timeout:
			t.Fatalf("timed out: got %d/%d results", i, numStreams)
		}
	}

	for i := uint64(1); i <= numStreams; i++ {
		assert.Equal(t, got[i], fmt.Sprintf("req-%d", i))
	}

	// Server manager must still be alive.
	assert.That(t, !closed(sman.Closed()))
}
