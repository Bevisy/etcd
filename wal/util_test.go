package wal

import (
	"testing"
)

func TestWalName(t *testing.T) {
	t.Error(walName(0, 0))
}
