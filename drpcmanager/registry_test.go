// Copyright (C) 2026 Cockroach Labs.
// See LICENSE for copying information.

package drpcmanager

import (
	"context"
	"testing"

	"github.com/zeebo/assert"
	"storj.io/drpc/drpcstream"
	"storj.io/drpc/drpcwire"
)

func testStream(id uint64) *drpcstream.Stream {
	return drpcstream.New(context.Background(), id, &drpcwire.Writer{})
}

// testActiveStreams returns an activeStreams with fresh term and tport signals.
func testActiveStreams() (*activeStreams, *Manager) {
	m := &Manager{}
	m.activeStreams = newActiveStreams(&m.sigs.term, &m.sigs.tport)
	return m.activeStreams, m
}

func TestActiveStreams_AddAndLatest(t *testing.T) {
	a, _ := testActiveStreams()
	s := testStream(1)

	assert.NoError(t, a.Add(1, s))

	got := a.Latest()
	assert.Equal(t, got, s)
}

func TestActiveStreams_Remove(t *testing.T) {
	a, _ := testActiveStreams()
	s := testStream(1)

	assert.NoError(t, a.Add(1, s))
	assert.That(t, a.Latest() != nil)

	a.Remove(1)

	assert.Nil(t, a.Latest())
}

func TestActiveStreams_RemoveIdempotent(t *testing.T) {
	a, _ := testActiveStreams()

	// must not panic when removing a non-existent ID
	a.Remove(99)
}

func TestActiveStreams_DuplicateAdd(t *testing.T) {
	a, _ := testActiveStreams()
	s1 := testStream(1)
	s2 := testStream(1)

	assert.NoError(t, a.Add(1, s1))
	assert.Error(t, a.Add(1, s2))

	// original stream is still present
	got := a.Latest()
	assert.Equal(t, got, s1)
}

func TestActiveStreams_AddAfterTerminate(t *testing.T) {
	a, m := testActiveStreams()
	m.sigs.term.Set(managerClosed.New("test"))

	err := a.Add(1, testStream(1))
	assert.Error(t, err)
}

func TestActiveStreams_RemoveAfterTerminate(t *testing.T) {
	a, m := testActiveStreams()
	s := testStream(1)
	assert.NoError(t, a.Add(1, s))

	m.sigs.term.Set(managerClosed.New("test"))

	// must not panic
	a.Remove(1)
}

func TestActiveStreams_LatestTracksNewest(t *testing.T) {
	a, _ := testActiveStreams()
	s1 := testStream(1)
	s2 := testStream(2)

	assert.NoError(t, a.Add(1, s1))
	assert.Equal(t, a.Latest(), s1)

	assert.NoError(t, a.Add(2, s2))
	assert.Equal(t, a.Latest(), s2)

	// Removing the old stream doesn't affect Latest.
	a.Remove(1)
	assert.Equal(t, a.Latest(), s2)
}

func TestActiveStreams_EmptyLatest(t *testing.T) {
	a, _ := testActiveStreams()
	assert.Nil(t, a.Latest())
}

func TestActiveStreams_Get(t *testing.T) {
	a, _ := testActiveStreams()
	s := testStream(1)

	assert.NoError(t, a.Add(1, s))

	got, ok := a.Get(1)
	assert.That(t, ok)
	assert.Equal(t, got, s)

	_, ok = a.Get(99)
	assert.That(t, !ok)
}

func TestActiveStreams_Len(t *testing.T) {
	a, _ := testActiveStreams()
	assert.Equal(t, a.Len(), 0)

	assert.NoError(t, a.Add(1, testStream(1)))
	assert.Equal(t, a.Len(), 1)

	assert.NoError(t, a.Add(2, testStream(2)))
	assert.Equal(t, a.Len(), 2)

	a.Remove(1)
	assert.Equal(t, a.Len(), 1)
}

func TestActiveStreams_Close(t *testing.T) {
	a, m := testActiveStreams()
	s1 := testStream(1)
	s2 := testStream(2)

	assert.NoError(t, a.Add(1, s1))
	assert.NoError(t, a.Add(2, s2))

	m.sigs.term.Set(managerClosed.New("test"))
	m.sigs.tport.Set(nil)
	a.Close(context.Canceled)

	// All streams are canceled.
	assert.That(t, s1.IsTerminated())
	assert.That(t, s2.IsTerminated())

	// Map is cleared.
	assert.Equal(t, a.Len(), 0)
	assert.Nil(t, a.Latest())
}

func TestActiveStreams_RemoveAfterClose(t *testing.T) {
	a, m := testActiveStreams()
	assert.NoError(t, a.Add(1, testStream(1)))

	m.sigs.term.Set(managerClosed.New("test"))
	m.sigs.tport.Set(nil)
	a.Close(context.Canceled)

	// must not panic
	a.Remove(1)
}

func TestActiveStreams_ClosePanicsWithoutTransportClose(t *testing.T) {
	a, _ := testActiveStreams()

	defer func() {
		r := recover()
		assert.That(t, r != nil)
	}()

	a.Close(context.Canceled)
}
