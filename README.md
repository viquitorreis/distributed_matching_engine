# Distributed Matching Engine

A low-latency order book and matching engine in Go, evolving from a
single node implementation into a distributed system with quorum based
replication and failover.

**Current state as of 21/jul:** single-node, skip list + doubly linked list price levels
(O(log n) cancel, O(1) order removal within a level). See [BENCHMARKS.md]
for a full comparison against an earlier heap-based implementation.

## Benchmark: Cancel Performance vs. the Original Heap-Based Implementation

Compared against the [original heap + slice implementation](https://github.com/viquitorreis/go-challenges/tree/main/11-exchange_order_book),
using the same two scenarios, same sizes, same methodology
(`go test -bench=. -benchmem -benchtime=100x`).

**Note**: benchmarks were performed on a amd64 12th Gen Intel(R) Core(TM) i5-1235U 32GB RAM machine

### Scenario A: cancelling a single order in the middle of a large price level

This targets the doubly linked list rewrite directly: a slice can't remove a
middle element without scanning and rebuilding it, a linked list can, given a
pointer to the node.

| Level depth | Old (ns/op) | New (ns/op) | Speedup |
|---|---|---|---|
| 10 | 1,342 | 179.8 | 7.5x |
| 100 | 3,806 | 639.5 | 6.0x |
| 1,000 | 21,912 | 712.5 | 30.8x |
| 10,000 | 127,790 | 1,112 | **114.9x** |

The old implementation grows close to linearly with level depth, matching
the expected O(n) cost of rebuilding the slice on every cancellation. The
new one stays close to flat. The small growth it does show (180ns to
1,112ns) isn't algorithmic: `list.Remove` is a true O(1) operation regardless
of list size. It's more likely attributable to GC overhead correlating with
total live heap size at larger scales, not to the cancel operation itself
doing more work. Worth calling out explicitly since it's a common source of
confusion when reading wall-clock benchmarks: flat algorithmic complexity
doesn't always mean flat wall-clock time once GC enters the picture.

Allocations confirm the same story: the old implementation allocates more per
cancel as the level grows (2 to 15 allocs/op, from `append` reallocating the
rebuilt slice), while the new implementation allocates **zeroj** per cancel at
every size, since `list.Remove` never allocates.

### Scenario B: cancelling N orders, each in its own price level

This targets the skip list rewrite directly: a heap can't remove an arbitrary
price level without a full rebuild, a skip list can, in O(log n).

Raw numbers represent the total cost of cancelling all N orders in one pass:

| Distinct levels | Old total (ns) | New total (ns) | Speedup |
|---|---|---|---|
| 10 | 4,049 | 3,280 | 1.2x |
| 100 | 153,935 | 33,494 | 4.6x |
| 1,000 | 8,683,372 | 309,355 | 28.1x |
| 10,000 | 978,893,452 | 4,054,022 | **241.4x** |

Normalized to cost per individual cancel (total ÷ N), the difference in
growth pattern is clearer:

| Distinct levels | Old (ns/cancel) | New (ns/cancel) |
|---|---|---|
| 10 | 404.9 | 328.0 |
| 100 | 1,539.4 | 334.9 |
| 1,000 | 8,683.4 | 309.4 |
| 10,000 | 97,889.3 | 405.4 |

The old implementation's per-cancel cost grows roughly 240x from N=10 to
N=10,000, consistent with each cancellation triggering an O(n) heap rebuild
(so the total cost of cancelling N distinct levels approaches O(n²)). The new
implementation's per-cancel cost stays essentially flat (~300-400ns)
regardless of N, consistent with O(log n) per deletion, where the log factor
is negligible at these scales.

At 10,000 price levels, cancelling all of them takes about 979ms with the
old implementation and about 4ms with the new one. That gap widens, not
narrows, as N grows further, since the two implementations are on different
complexity classes (O(n²) vs. O(n log n) for cancelling all N levels), not
just different constants.

Allocations tell a matching story: the old implementation's allocations per
cancel grow with N (4 to 16.8 allocs/cancel), from the heap's backing array
being reallocated on rebuild. The new implementation holds constant at
**exactly 1 allocation per cancel**, regardless of N, coming from the fixed-size
`predecessors` slice (`make([]*SkipListNode, maxLevel)`) allocated on every
`Delete` call, independent of how many elements are actually in the skip
list.

### Takeaway

Both rewritten tracks hold up under measurement, not just under theoretical
complexity analysis. Track B (doubly linked list) delivers consistent,
allocation-free O(1) cancellation regardless of price level depth. Track A
(skip list) turns what was an increasingly expensive O(n²) total cost for
churny order books (many price levels, frequent cancellations) into a
near-linear O(n log n) cost, a 241x improvement at 10,000 price levels that
keeps growing with scale.