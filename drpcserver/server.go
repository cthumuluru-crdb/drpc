// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package drpcserver

import (
	"context"
	"crypto/tls"
	"log"
	"net"
	"sync"
	"time"

	"github.com/zeebo/errs"
	"storj.io/drpc"
	"storj.io/drpc/drpccache"
	"storj.io/drpc/drpcctx"
	"storj.io/drpc/drpcmanager"
	"storj.io/drpc/drpcstats"
	"storj.io/drpc/drpcstream"
	"storj.io/drpc/internal/drpcopts"
)

// Options controls configuration settings for a server.
type Options struct {
	// Manager controls the options we pass to the managers this server creates.
	Manager drpcmanager.Options

	// Log is called when errors happen that can not be returned up, like
	// temporary network errors when accepting connections, or errors
	// handling individual clients. It is not called if nil.
	Log func(error)

	// CollectStats controls whether the server should collect stats on the
	// rpcs it serves.
	CollectStats bool
}

// Server is an implementation of drpc.Server to serve drpc connections.
type Server struct {
	opts    Options
	handler drpc.Handler

	mu    sync.Mutex
	stats map[string]*drpcstats.Stats
}

// New constructs a new Server.
func New(handler drpc.Handler) *Server {
	return NewWithOptions(handler, Options{})
}

// NewWithOptions constructs a new Server using the provided options to tune
// how the drpc connections are handled.
func NewWithOptions(handler drpc.Handler, opts Options) *Server {
	s := &Server{
		opts:    opts,
		handler: handler,
	}

	if s.opts.CollectStats {
		drpcopts.SetManagerStatsCB(&s.opts.Manager.Internal, s.getStats)
		s.stats = make(map[string]*drpcstats.Stats)
	}

	return s
}

// Stats returns the collected stats grouped by rpc.
func (s *Server) Stats() map[string]drpcstats.Stats {
	s.mu.Lock()
	defer s.mu.Unlock()

	stats := make(map[string]drpcstats.Stats, len(s.stats))
	for k, v := range s.stats {
		stats[k] = v.AtomicClone()
	}
	return stats
}

// getStats returns the drpcopts.Stats struct for the given rpc.
func (s *Server) getStats(rpc string) *drpcstats.Stats {
	s.mu.Lock()
	defer s.mu.Unlock()

	stats := s.stats[rpc]
	if stats == nil {
		stats = new(drpcstats.Stats)
		s.stats[rpc] = stats
	}
	return stats
}

// ServeOne serves a single set of rpcs on the provided transport.
func (s *Server) ServeOne(ctx context.Context, tr drpc.Transport) (err error) {
	// Check if the transport is a TLS connection
	if tlsConn, ok := tr.(*tls.Conn); ok {
		// Manually perform the TLS handshake to access peer certificate
		// information. In Go's TLS implementation, the handshake is normally
		// performed lazily on the first read/write operation. However, the
		// transport received by ServeOne hasn't performed any I/O yet, so
		// ConnectionState() would be empty. Only after the handshake completes
		// is ConnectionState populated with peer certificates and other
		// connection details that we need for authentication context.
		//
		// This explicit Handshake() call is safe and appropriate here. The
		// connection hasn't started processing requests yet, so we're not
		// interrupting any ongoing communication. Even if we didn't call it
		// explicitly, the first read/write operation would call it internally
		// anyway.
		err := tlsConn.Handshake()
		if err != nil {
			return err
		}
		state := tlsConn.ConnectionState()
		if len(state.PeerCertificates) > 0 {
			ctx = drpcctx.WithPeerConnectionInfo(
				ctx, drpcctx.PeerConnectionInfo{Certificates: state.PeerCertificates})
		}
	}

	// TODO(chandrat): generate a unique connection ID.
	connID := make([]byte, 8)
	n, err := tr.Read(connID)
	if err != nil || n < 8 {
		tr.Close()
		return errs.New("drpcserver: failed to read connection ID")
	}
	s.opts.Manager.ConnID = string(connID)

	log.Printf("[ServeOne] connID[%s]: starting DRPC manager", string(connID))
	man := drpcmanager.NewWithOptions(tr, s.opts.Manager)
	defer func() {
		err = errs.Combine(err, man.Close())
		log.Printf("[ServeOne] connID[%s]: DRPC manager closed with error: %v", string(connID), err)
	}()

	cache := drpccache.New()
	defer cache.Clear()

	ctx = drpccache.WithContext(ctx, cache)

	for {
		stream, rpc, err := man.NewServerStream(ctx)
		if err != nil {
			log.Printf("[ServeOne] connID[%s]: error creating new stream: %v", string(connID), err)
			return errs.Wrap(err)
		}
		if err := s.handleRPC(stream, rpc); err != nil {
			log.Printf("[ServeOne] connID[%s]: error handling rpc [%s]: %v", string(connID), rpc, err)
			return errs.Wrap(err)
		}
	}
}

var temporarySleep = 500 * time.Millisecond

// Serve listens for connections on the listener and serves the drpc request
// on new connections.
func (s *Server) Serve(ctx context.Context, lis net.Listener) (err error) {
	tracker := drpcctx.NewTracker(ctx)
	defer tracker.Wait()
	defer tracker.Cancel()

	tracker.Run(func(ctx context.Context) {
		<-ctx.Done()
		_ = lis.Close()
	})

	for {
		conn, err := lis.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}

			if isTemporary(err) {
				if s.opts.Log != nil {
					s.opts.Log(err)
				}

				t := time.NewTimer(temporarySleep)
				select {
				case <-t.C:
				case <-ctx.Done():
					t.Stop()
					return nil
				}

				continue
			}

			return errs.Wrap(err)
		}

		// TODO(jeff): connection limits?
		tracker.Run(func(ctx context.Context) {
			err := s.ServeOne(ctx, conn)
			if err != nil && s.opts.Log != nil {
				s.opts.Log(err)
			}
		})
	}
}

// func (s *Server) handleRPC(stream *drpcstream.Stream, connID, rpc string) (err error) {
// 	if rpc == "/cockroach.roachpb.KVBatch/Batch" {
// 		log.Printf("[handleRPC] connID[%s] rpc[%s]: begin", connID, rpc)
// 	}
// 	err = s.doHandleRPC(stream, rpc)
// 	if rpc == "/cockroach.roachpb.KVBatch/Batch" {
// 		if err != nil {
// 			log.Printf("[handleRPC] connID[%s] rpc[%s]: failed: %v", connID, rpc, err)
// 		} else {
// 			log.Printf("[handleRPC] connID[%s] rpc[%s]: success", connID, rpc)
// 		}
// 	}
// 	return err
// }

// handleRPC handles the rpc that has been requested by the stream.
func (s *Server) handleRPC(stream *drpcstream.Stream, rpc string) (err error) {
	err = s.handler.HandleRPC(stream, rpc)
	if err != nil {
		return errs.Wrap(stream.SendError(err))
	}
	return errs.Wrap(stream.CloseSend())
}
