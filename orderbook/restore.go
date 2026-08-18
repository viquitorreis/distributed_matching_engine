package orderbook

import (
	"bytes"
	"container/list"
	"encoding/gob"
	"fmt"
)

// Restore replaces this order books entire state with the given
// snapshot. Meant for a node catching up after rejoining wipes
// whatever local state exists first, since the snapshot is the source
// of truth from a peer that's ahead.
func (ob *OrderBook) Restore(data []byte) error {
	var orders []Order
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&orders); err != nil {
		return fmt.Errorf("decode snapshot: %w", err)
	}

	ob.mu.Lock()
	defer ob.mu.Unlock()

	// wipe current state same fresh structures NewOrderBook would create
	ob.bidsIndex = NewSkipList(conf.maxLevel, Bid, conf.probability, conf.rng)
	ob.asksIndex = NewSkipList(conf.maxLevel, Ask, conf.probability, conf.rng)
	ob.tracker = make(map[string]*list.Element)

	for _, o := range orders {
		full := NewOrder(o.ID, "", o.Side, o.Price, o.Quantity) // UserID not in snapshot today, see note below
		full.Timestamp = 0                                      // ordering within a price level is lost across snapshot, todo: documented limitation
		ob.AddOrder(full)
	}

	return nil
}

func (ob *OrderBook) Snapshot() ([]byte, error) {
	orders := ob.AllOrders()

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(orders); err != nil {
		return nil, fmt.Errorf("encode snapshot: %w", err)
	}

	return buf.Bytes(), nil
}
