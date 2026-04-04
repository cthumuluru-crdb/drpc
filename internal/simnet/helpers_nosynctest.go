//go:build !synctest

package simnet

import "testing"

func runInBubble(t *testing.T, f func(*testing.T)) {
	f(t)
}
