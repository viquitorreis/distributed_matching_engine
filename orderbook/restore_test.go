package orderbook

import (
	"testing"
)

func TestSnapshotRestore_RoundTrip(t *testing.T) {
	ob := NewOrderBook("BTC-USD")

	// populate with a few orders on both sides, deliberately not
	// crossing (different prices), so nothing matches away before
	// the snapshot is taken
	ob.AddOrder(NewOrder("bid-1", "user-a", Bid, 100, 10))
	ob.AddOrder(NewOrder("bid-2", "user-b", Bid, 95, 5))
	ob.AddOrder(NewOrder("ask-1", "user-c", Ask, 110, 7))

	snap, err := ob.Snapshot()
	if err != nil {
		t.Fatalf("snapshot failed: %v", err)
	}

	restored := NewOrderBook("BTC-USD")
	if err := restored.Restore(snap); err != nil {
		t.Fatalf("restore failed: %v", err)
	}

	original := ob.AllOrders()
	after := restored.AllOrders()

	if len(original) != len(after) {
		t.Fatalf("order count mismatch: original=%d restored=%d", len(original), len(after))
	}

	// build a lookup by ID since AllOrders' iteration order isn't
	// guaranteed to match between the two books (skip list levels
	// aren't necessarily walked in the same order twice)
	byID := make(map[string]Order, len(after))
	for _, o := range after {
		byID[o.ID] = o
	}

	for _, want := range original {
		got, ok := byID[want.ID]
		if !ok {
			t.Fatalf("order %s missing after restore", want.ID)
		}
		if got.Side != want.Side || got.Price != want.Price || got.Quantity != want.Quantity {
			t.Errorf("order %s mismatch: got %+v, want %+v", want.ID, got, want)
		}
	}
}

func TestRestore_WipesExistingState(t *testing.T) {
	ob := NewOrderBook("BTC-USD")
	ob.AddOrder(NewOrder("stale-order", "user-x", Bid, 50, 3))

	// snapshot from a DIFFERENT, empty book restoring this should
	// wipe "stale-order", not merge with it
	empty := NewOrderBook("BTC-USD")
	snap, err := empty.Snapshot()
	if err != nil {
		t.Fatalf("snapshot failed: %v", err)
	}

	if err := ob.Restore(snap); err != nil {
		t.Fatalf("restore failed: %v", err)
	}

	if orders := ob.AllOrders(); len(orders) != 0 {
		t.Fatalf("expected empty book after restoring an empty snapshot, got %d orders", len(orders))
	}
}
