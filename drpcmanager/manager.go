// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package drpcmanager

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zeebo/errs"
	grpcmetadata "google.golang.org/grpc/metadata"
	"storj.io/drpc"
	"storj.io/drpc/drpcdebug"
	"storj.io/drpc/drpcmetadata"
	"storj.io/drpc/drpcsignal"
	"storj.io/drpc/drpcstream"
	"storj.io/drpc/drpcwire"
	"storj.io/drpc/internal/drpcopts"
)

var managerClosed = errs.Class("manager closed")

// Options controls configuration settings for a manager.
type Options struct {
	// WriterBufferSize controls the size of the buffer that we will fill before
	// flushing. Normal writes to streams typically issue a flush explicitly.
	WriterBufferSize int

	// Reader are passed to any readers the manager creates.
	Reader drpcwire.ReaderOptions

	// Stream are passed to any streams the manager creates.
	Stream drpcstream.Options

	// InactivityTimeout is the amount of time the manager will wait for
	// the first invoke frame from the remote side. If no invoke frame is
	// received within this duration, the manager terminates. If zero or
	// negative, no timeout is used.
	InactivityTimeout time.Duration

	// Internal contains options that are for internal use only.
	Internal drpcopts.Manager

	// ServerContext, when set, is used as the base context for server-side
	// streams.
	ServerContext context.Context

	// ServerHandler, when set, is called in a new goroutine for each
	// incoming server-side RPC. The handler receives the stream and the
	// RPC method name. If nil, invoke frames will cause a protocol error.
	ServerHandler func(stream *drpcstream.Stream, rpc string)

	// GRPCMetadataCompatMode enables/disable gRPC compatibility for metadata
	// handling. When enabled, the server stream will decode incoming metadata
	// into grpc metadata in the context.
	GRPCMetadataCompatMode bool
}

// Manager handles the logic of managing a transport for a drpc client or
// server. It ensures that the connection is always being read from, that it is
// closed in the case that the manager is and forwarding drpc protocol messages
// to the appropriate stream.
type Manager struct {
	tr   drpc.Transport
	wr   *drpcwire.Writer
	rd   *drpcwire.Reader
	opts Options

	// next client stream ID, incremented atomically
	lastStreamID atomic.Uint64

	wg sync.WaitGroup // tracks active manageStream goroutines

	// activeStreams tracks activeStreams streams. It checks sigs.term atomically with Add
	// to prevent TOCTOU races.
	activeStreams *activeStreams
	// Below fields are owned by the manageReader goroutine, used in handleInvokeFrame.
	pendingStreams     map[uint64]*pendingStream // per-stream invoke assembly state
	lastInvokeStreamID uint64                    // highest stream ID seen in an invoke; enforces monotonicity

	sigs struct {
		term  drpcsignal.Signal // set when the manager should start terminating
		read  drpcsignal.Signal // set after the goroutine reading from the transport is done
		tport drpcsignal.Signal // set after the transport has been closed
	}
}

// pendingStream tracks per-stream invoke packet assembly state. Invoke and
// metadata frames are accumulated here until the invoke packet completes the
// sequence and a stream is created. Owned exclusively by the manageReader
// goroutine.
type pendingStream struct {
	pa       drpcwire.PacketAssembler
	metadata map[string]string
}

// New returns a new Manager for the transport.
func New(tr drpc.Transport) *Manager {
	return NewWithOptions(tr, Options{})
}

// NewWithOptions returns a new manager for the transport. It uses the provided
// options to manage details of how it uses it.
func NewWithOptions(tr drpc.Transport, opts Options) *Manager {
	m := &Manager{
		tr:   tr,
		wr:   drpcwire.NewWriter(tr, opts.WriterBufferSize),
		rd:   drpcwire.NewReaderWithOptions(tr, opts.Reader),
		opts: opts,
	}

	m.pendingStreams = make(map[uint64]*pendingStream)
	m.activeStreams = newActiveStreams(&m.sigs.term, &m.sigs.tport)

	// set the internal stream options
	drpcopts.SetStreamTransport(&m.opts.Stream.Internal, m.tr)

	go m.manageReader()

	return m
}

// String returns a string representation of the manager.
func (m *Manager) String() string { return fmt.Sprintf("<man %p>", m) }

func (m *Manager) log(what string, cb func() string) {
	if drpcdebug.Enabled {
		drpcdebug.Log(func() (_, _, _ string) { return m.String(), what, cb() })
	}
}

//
// helpers
//

// terminate puts the Manager into a terminal state and closes any resources
// that need to be closed to signal the state change.
func (m *Manager) terminate(err error) {
	if m.sigs.term.Set(err) {
		m.log("TERM", func() string { return fmt.Sprint(err) })
		m.sigs.tport.Set(m.tr.Close())
		m.activeStreams.Close(err)
	}
}

//
// manage reader
//

// manageReader reads the frame and dispatches them to the appropriate stream or
// queue. It sets the read signal when it exits so that one can wait to ensure
// that no one is reading on the reader. It sets the term signal if there is any
// error reading frames.
func (m *Manager) manageReader() {
	defer m.sigs.read.Set(nil)

	for !m.sigs.term.IsSet() {
		incomingFrame, err := m.rd.ReadFrame()
		if err != nil {
			// Any read error means the transport is broken. Wrap with
			// ClosedError so that ToRPCErr maps it to codes.Unavailable,
			// matching gRPC's behavior for transport read failures.
			err = drpc.ClosedError.Wrap(err)
			m.terminate(managerClosed.Wrap(err))
			return
		}

		m.log("READ", incomingFrame.String)

		switch stream, found := m.activeStreams.Get(incomingFrame.ID.Stream); {
		// If the frame belongs to an active stream, deliver it.
		case found:
			if err := stream.HandleFrame(incomingFrame); err != nil {
				m.terminate(managerClosed.Wrap(err))
				return
			}

		// If an invoke sequence is being sent for a new stream, forward it.
		case incomingFrame.Kind == drpcwire.KindInvoke || incomingFrame.Kind == drpcwire.KindInvokeMetadata:
			if err := m.handleInvokeFrame(incomingFrame); err != nil {
				m.terminate(managerClosed.Wrap(err))
				return
			}

		default:
			// No active stream for this ID and it's not an invoke. This
			// can happen when a stream has been removed (e.g. context
			// canceled) while frames are still in flight. Silently ignore.
		}
	}
}

// handleInvokeFrame assembles invoke/metadata frames into complete packets,
// creates the server stream, and dispatches to the ServerHandler callback.
// Metadata packets are accumulated per-stream; the invoke packet triggers
// stream creation. Invoke frames for different streams may interleave on
// the wire.
func (m *Manager) handleInvokeFrame(fr drpcwire.Frame) error {
	var ps *pendingStream
	switch eps, found := m.pendingStreams[fr.ID.Stream]; {
	case found:
		// handleInvokeFrame is only for assembling invoke sequences, so all
		// frames must be either invoke metadata or invoke. If we already found
		// a pending stream, then we must be in the middle of assembling an
		// invoke sequence, so the new frame must invoke.
		if fr.Kind != drpcwire.KindInvoke {
			return drpc.ProtocolError.New(
				"invoke sequence frame kind violation: got %d, expected %d",
				fr.Kind, drpcwire.KindInvoke)
		}
		ps = eps
	default:
		// This is a new stream request. New stream IDs must be strictly
		// increasing.
		if fr.ID.Stream <= m.lastInvokeStreamID {
			return drpc.ProtocolError.New(
				"invoke stream id monotonicity violation: got %d, expected > %d",
				fr.ID.Stream, m.lastInvokeStreamID)
		}

		ps = &pendingStream{}
		ps.pa.SetStreamID(fr.ID.Stream)
		m.pendingStreams[fr.ID.Stream] = ps
		m.lastInvokeStreamID = fr.ID.Stream
	}

	pkt, packetReady, err := ps.pa.AppendFrame(fr)
	if err != nil {
		return err
	}
	if !packetReady {
		return nil
	}

	// Metadata arrives before invoke; accumulate it and wait for the invoke.
	if pkt.Kind == drpcwire.KindInvokeMetadata {
		meta, err := drpcmetadata.Decode(pkt.Data)
		if err != nil {
			return err
		}
		ps.metadata = meta
		return nil
	}

	// TODO(server): The following constraints are not strictly necessary.
	// We should panic but we will defer that until we separate manager into
	// client and server managers.
	//
	// Invoke packet completes the sequence. Create stream and dispatch.
	if m.opts.ServerHandler == nil {
		return drpc.InternalError.New("invoke received but no ServerHandler configured")
	}
	ctx := m.opts.ServerContext
	if ctx == nil {
		ctx = context.Background()
	}

	if ps.metadata != nil {
		if m.opts.GRPCMetadataCompatMode {
			grpcMeta := make(map[string][]string, len(ps.metadata))
			for k, v := range ps.metadata {
				grpcMeta[k] = []string{v}
			}
			ctx = grpcmetadata.NewIncomingContext(ctx, grpcMeta)
		} else {
			ctx = drpcmetadata.NewIncomingContext(ctx, ps.metadata)
		}
	}

	rpc := string(pkt.Data)
	stream, err := m.newStream(ctx, pkt.ID.Stream, drpc.StreamKindServer, rpc)
	if err != nil {
		return err
	}
	// TODO(server): we should remove this regardless of server stream creation
	// success.
	delete(m.pendingStreams, pkt.ID.Stream)

	m.wg.Add(1)
	go m.manageStream(ctx, stream)
	go m.opts.ServerHandler(stream, rpc)

	return nil
}

//
// manage streams
//

// newStream creates a stream value with the appropriate configuration for this manager.
func (m *Manager) newStream(
	ctx context.Context, sid uint64, kind drpc.StreamKind, rpc string,
) (*drpcstream.Stream, error) {
	opts := m.opts.Stream
	drpcopts.SetStreamKind(&opts.Internal, kind)
	drpcopts.SetStreamRPC(&opts.Internal, rpc)
	if cb := drpcopts.GetManagerStatsCB(&m.opts.Internal); cb != nil {
		drpcopts.SetStreamStats(&opts.Internal, cb(rpc))
	}

	stream := drpcstream.NewWithOptions(ctx, sid, m.wr, opts)
	if err := m.activeStreams.Add(sid, stream); err != nil {
		return nil, err
	}

	m.log("STREAM", stream.String)

	return stream, nil
}

// manageStream watches the context and the stream and returns when the stream
// is finished, canceling the stream if the context is canceled.
func (m *Manager) manageStream(ctx context.Context, stream *drpcstream.Stream) {
	defer m.wg.Done()
	defer m.activeStreams.Remove(stream.ID())
	select {
	case <-stream.Finished():
		// stream completed normally

	case <-ctx.Done():
		m.log("CANCEL", stream.String)
		stream.Cancel(ctx.Err())
	}
}

//
// exported interface
//

// Closed returns a channel that is closed once the manager is closed.
func (m *Manager) Closed() <-chan struct{} {
	return m.sigs.term.Signal()
}

// Unblocked returns a channel that is closed when the manager is no longer
// blocked from creating a new stream. With multiplexing support, streams no
// longer block each other, so this always returns an already-closed channel.
func (m *Manager) Unblocked() <-chan struct{} {
	return closedCh
}

// Close closes the transport the manager is using.
func (m *Manager) Close() error {
	m.terminate(managerClosed.New("Close called"))

	m.wg.Wait() // wait for all stream goroutines
	m.sigs.read.Wait()
	m.sigs.tport.Wait()

	return m.sigs.tport.Err()
}

// NewClientStream starts a stream on the managed transport for use by a client.
func (m *Manager) NewClientStream(
	ctx context.Context, rpc string,
) (stream *drpcstream.Stream, err error) {
	if err, ok := m.sigs.term.Get(); ok {
		return nil, err
	} else if err := ctx.Err(); err != nil {
		return nil, err
	}

	stream, err = m.newStream(ctx, m.lastStreamID.Add(1), drpc.StreamKindClient, rpc)
	if err != nil {
		return nil, err
	}

	m.wg.Add(1)
	go m.manageStream(ctx, stream)

	return stream, nil
}
