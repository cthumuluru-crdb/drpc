// Copyright (C) 2024 Storj Labs, Inc.
// See LICENSE for copying information.

// Package drpcquic adapts DRPC onto QUIC: each DRPC stream maps to its own QUIC
// stream, so QUIC's native, independent streams provide the multiplexing (and
// eliminate the head-of-line blocking inherent to multiplexing many logical
// streams over a single byte transport). The DRPC framing, encoding, error and
// metadata helpers are reused on each QUIC stream; only DRPC's own multiplexing
// manager is bypassed.
package drpcquic

import (
	"crypto/tls"

	"github.com/quic-go/quic-go"
)

// applicationProtocol is the ALPN identifier used for DRPC-over-QUIC. QUIC
// mandates an application protocol, so we set one internally rather than
// exposing it as configuration.
const applicationProtocol = "drpc"

// defaultMaxMessageSize bounds the size of a single message read off a QUIC
// stream. Because each message is written as a single frame, this is the one
// remaining reason to cap a frame (defense against a malicious peer); QUIC
// itself handles packetization and flow control.
const defaultMaxMessageSize = 4 << 20

// quicCancelCode is the QUIC stream error code used when abruptly canceling a
// stream's read or write side.
const quicCancelCode quic.StreamErrorCode = 0

// quicConnCloseCode is the QUIC application error code used when closing a
// connection.
const quicConnCloseCode quic.ApplicationErrorCode = 0

// ensureALPN returns a clone of tlsConf whose ALPN protocol list is exactly the
// DRPC application protocol. QUIC negotiates the application protocol via ALPN,
// so both client and server must agree; we force our protocol (overriding any
// other protocols, e.g. "h2", that the base TLS config may list) so the
// handshake succeeds regardless of how the caller's config was built.
func ensureALPN(tlsConf *tls.Config) *tls.Config {
	if tlsConf == nil {
		tlsConf = &tls.Config{}
	} else {
		tlsConf = tlsConf.Clone()
	}
	tlsConf.NextProtos = []string{applicationProtocol}
	// Some configs (e.g. CockroachDB's server config) perform the handshake using
	// a config returned from GetConfigForClient for cert hot-reloading, which
	// would otherwise drop our NextProtos. Wrap it so the dynamically-selected
	// config also advertises our ALPN protocol.
	if base := tlsConf.GetConfigForClient; base != nil {
		tlsConf.GetConfigForClient = func(chi *tls.ClientHelloInfo) (*tls.Config, error) {
			cfg, err := base(chi)
			if err != nil || cfg == nil {
				return cfg, err
			}
			cfg = cfg.Clone()
			cfg.NextProtos = []string{applicationProtocol}
			return cfg, nil
		}
	}
	return tlsConf
}

// Listen starts a QUIC listener on addr using tlsConf. The DRPC application
// protocol is set automatically when tlsConf does not specify one.
func Listen(addr string, tlsConf *tls.Config) (*quic.Listener, error) {
	// NOTE: we use the 1-RTT quic.ListenAddr, not the 0-RTT "early" variant
	// (quic.ListenAddrEarly). See the matching note in Dial: 0-RTT early data is
	// replayable and is deliberately deferred for now.
	return quic.ListenAddr(addr, ensureALPN(tlsConf), nil)
}
