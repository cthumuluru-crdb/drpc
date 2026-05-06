// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package drpc

import (
	"strings"
	"testing"

	"github.com/zeebo/assert"
)

func TestDefaultLogger_ImplementsInterface(t *testing.T) {
	var _ Logger = DefaultLogger
}

func TestDefaultLogger_DebugfIsNoop(t *testing.T) {
	// Debugf should not panic or produce output.
	DefaultLogger.Debugf("this should be a no-op: %d", 42)
}

func TestInMemLogger_ImplementsInterface(t *testing.T) {
	var _ Logger = (*InMemLogger)(nil)
}

func TestInMemLogger_CapturesAllLevels(t *testing.T) {
	var l InMemLogger

	l.Debugf("debug %d", 1)
	l.Infof("info %d", 2)
	l.Errorf("error %d", 3)
	l.Fatalf("fatal %d", 4)

	got := l.String()
	assert.That(t, strings.Contains(got, "DEBUG: debug 1"))
	assert.That(t, strings.Contains(got, "INFO: info 2"))
	assert.That(t, strings.Contains(got, "ERROR: error 3"))
	assert.That(t, strings.Contains(got, "FATAL: fatal 4"))
}

func TestInMemLogger_AppendsNewline(t *testing.T) {
	var l InMemLogger

	l.Infof("no newline")
	l.Infof("also no newline")

	lines := strings.Split(strings.TrimSpace(l.String()), "\n")
	assert.Equal(t, len(lines), 2)
}

func TestInMemLogger_PreservesExistingNewline(t *testing.T) {
	var l InMemLogger

	l.Infof("has newline\n")
	l.Infof("another")

	lines := strings.Split(strings.TrimSpace(l.String()), "\n")
	assert.Equal(t, len(lines), 2)
}

func TestInMemLogger_Reset(t *testing.T) {
	var l InMemLogger

	l.Infof("before reset")
	assert.That(t, l.String() != "")

	l.Reset()
	assert.Equal(t, l.String(), "")

	l.Infof("after reset")
	assert.That(t, strings.Contains(l.String(), "after reset"))
}
