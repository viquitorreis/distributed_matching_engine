package raft_test

import (
	"testing"

	"raft_orderbook/raft"
	"raft_orderbook/storage"
)

func TestFastForward_NeverMovesBackward(t *testing.T) {
	st := &storage.Storage{FilePath: t.TempDir() + "/state.json"}
	r := raft.NewRaft(3, st)

	r.FastForward(50)
	if got := r.LastLogIndex(); got != 50 {
		t.Fatalf("expected LastLogIndex=50, got %d", got)
	}

	// a stale/duplicate response arriving late with a lower index
	// must be a no-op, not a regression
	r.FastForward(30)
	if got := r.LastLogIndex(); got != 50 {
		t.Fatalf("expected LastLogIndex to stay at 50 after a lower FastForward, got %d", got)
	}

	// advancing further still works normally
	r.FastForward(75)
	if got := r.LastLogIndex(); got != 75 {
		t.Fatalf("expected LastLogIndex=75, got %d", got)
	}
}
