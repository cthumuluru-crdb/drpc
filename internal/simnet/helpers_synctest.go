//go:build synctest

package simnet

import (
	"testing"
	"testing/synctest"
)

func runInBubble(t *testing.T, f func(*testing.T)) {
	synctest.Test(t, f)
}
