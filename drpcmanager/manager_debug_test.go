// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

//go:build drpcdebug

package drpcmanager

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"

	"github.com/zeebo/assert"
	"storj.io/drpc"
	"storj.io/drpc/drpctest"
	"storj.io/drpc/drpcwire"
)

func TestManagerLoggerPropagation(t *testing.T) {
	ctx := drpctest.NewTracker(t)
	defer ctx.Close()

	cconn, sconn := net.Pipe()
	defer func() { _ = cconn.Close() }()
	defer func() { _ = sconn.Close() }()

	var logger drpc.InMemLogger

	cman := NewWithOptions(cconn, Options{Logger: &logger})
	defer func() { _ = cman.Close() }()

	sman := New(sconn)
	defer func() { _ = sman.Close() }()

	ctx.Run(func(ctx context.Context) {
		stream, err := cman.NewClientStream(ctx, "rpc")
		assert.NoError(t, err)
		defer func() { _ = stream.Close() }()

		assert.NoError(t, stream.RawWrite(drpcwire.KindInvoke, []byte("invoke")))
		assert.NoError(t, stream.RawWrite(drpcwire.KindMessage, []byte("message")))
		assert.NoError(t, stream.RawFlush())

		assert.NoError(t, stream.Close())
	})

	ctx.Run(func(ctx context.Context) {
		stream, _, err := sman.NewServerStream(ctx)
		assert.NoError(t, err)
		defer func() { _ = stream.Close() }()

		_, err = stream.RawRecv()
		assert.NoError(t, err)

		_, err = stream.RawRecv()
		assert.That(t, errors.Is(err, io.EOF))
	})

	ctx.Wait()

	logs := logger.String()
	assert.That(t, strings.Contains(logs, "DEBUG:"))
}
