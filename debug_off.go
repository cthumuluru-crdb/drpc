// Copyright (C) 2019 Storj Labs, Inc.
// See LICENSE for copying information.

//go:build !drpcdebug

package drpc

// DebugEnabled controls whether debug logging is active. When false (the
// default), the compiler eliminates debug log callsites entirely so that
// callbacks passed to log helpers are never allocated or evaluated.
const DebugEnabled = false
