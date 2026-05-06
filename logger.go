// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

package drpc

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"sync"
)

// Logger defines an interface for writing log messages.
type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
}

type defaultLogger struct{}

// DefaultLogger logs to the Go stdlib log package. Debugf is a no-op.
var DefaultLogger defaultLogger

var _ Logger = DefaultLogger

func (defaultLogger) Debugf(format string, args ...interface{}) {}

func (defaultLogger) Infof(format string, args ...interface{}) {
	_ = log.Output(2, fmt.Sprintf(format, args...))
}

func (defaultLogger) Errorf(format string, args ...interface{}) {
	_ = log.Output(2, fmt.Sprintf(format, args...))
}

func (defaultLogger) Fatalf(format string, args ...interface{}) {
	_ = log.Output(2, fmt.Sprintf(format, args...))
	os.Exit(1)
}

// InMemLogger implements Logger using an in-memory buffer (used for testing).
// The buffer can be read via String() and cleared via Reset().
type InMemLogger struct {
	mu struct {
		sync.Mutex
		buf bytes.Buffer
	}
}

var _ Logger = (*InMemLogger)(nil)

// Reset clears the internal buffer.
func (b *InMemLogger) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mu.buf.Reset()
}

// String returns the current internal buffer.
func (b *InMemLogger) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.mu.buf.String()
}

func (b *InMemLogger) writef(prefix, format string, args ...interface{}) {
	s := fmt.Sprintf(prefix+format, args...)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.mu.buf.WriteString(s)
	if len(s) == 0 || s[len(s)-1] != '\n' {
		b.mu.buf.WriteByte('\n')
	}
}

func (b *InMemLogger) Debugf(format string, args ...interface{}) {
	b.writef("DEBUG: ", format, args...)
}

func (b *InMemLogger) Infof(format string, args ...interface{}) {
	b.writef("INFO: ", format, args...)
}

func (b *InMemLogger) Errorf(format string, args ...interface{}) {
	b.writef("ERROR: ", format, args...)
}

func (b *InMemLogger) Fatalf(format string, args ...interface{}) {
	b.writef("FATAL: ", format, args...)
}
