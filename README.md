# Distributed Matching Engine

A low-latency order book and matching engine in Go, evolving from a
single node implementation into a distributed system with quorum based
replication and failover.

**State as of 21/jul:** single-node, skip list + doubly linked list price levels
(O(log n) cancel, O(1) order removal within a level). See [BENCHMARKS.md]
for a full comparison against an earlier heap-based implementation.

**Current state as of 14/ago**: distributed cluster over the original single-node engine, order book unchanged internally (skip list + doubly linked list price levels, O(log n) cancel, O(1) order removal within a level, see [BENCHMARKS.md] for the heap-based comparison), now wrapped in an Operation/Apply layer so commands replicate deterministically across nodes. Replication runs over a hand-rolled TCP protocol (length-prefix framing, one goroutine per peer for reads/writes, identity via handshake) with quorum-based write commits and Raft-based leader election (Follower/Candidate/Leader, randomized election timeout, term-based voting). Validated manually across 3 real nodes: cross-node order matching converges correctly, and a new leader is elected with a higher term after killing the current one. Known gaps: no total ordering across concurrent proposals yet (any node can still propose, not just the leader), Raft state is in-memory only (lost on restart), and no catch-up mechanism for a node rejoining after downtime.

## Architecture

Package breakdown, each with a single clear responsibility:

- orderbook: matching engine itself (skip list + doubly linked price
  levels). Unchanged internally gains a thin Operation/Apply layer
  so replicated commands (AddOrder/Cancel) can be re-applied
  deterministically on every node, with Match() re-run after each
  commit to converge to identical state across the cluster.

- framing: length-prefix wire protocol (4-byte size + payload),
  transport-agnostic, shared by every message type below.

- peer: owns a single TCP connection end-to-end. One goroutine for
  reads, one for writes, so no two goroutines ever touch the same
  conn concurrently. Identity is established via a blocking Hello
  handshake before any other message type, replacing an initial
  RemoteAddr-based identity that broke on accepted connections
  (ephemeral ports).

- cluster: peer registry and message routing. Tracks pending write
  proposals (nodeAddr:counter IDs, voter sets for idempotent acks),
  calculates quorum dynamically from live peer count, and commits an
  operation to the local order book once quorum is reached,
  broadcasting MsgCommit so every node applies the same command, not
  just the proposer.

- raft: pure consensus state machine, no networking, no knowledge of
  cluster/peer/orderbook. Implements leader election (Follower/
  Candidate/Leader states, randomized election timeout, term-based
  RequestVote/VoteGranted) and exposes QuorumSize() as the single
  source of truth for majority, shared between election and future
  log-commit logic. Testable in isolation without sockets.

- cmd/node: composition root. Wires framing + orderbook + cluster +
  raft together, handles peer dial/listen with a lexicographic rule
  to avoid duplicate connections, and exposes a CLI for placing and
  cancelling orders.

Known limitations, documented rather than hidden:

- No total ordering across concurrent proposals from different nodes
  yet any node can still propose, not just the elected leader; real
  Raft log replication (index + term per entry, leader-only proposal)
  is the next step
- Raft state (term, votedFor) is in-memory only, lost on restart
  persistence needed before this is safe against split-vote-after-crash
- No catch-up/snapshot mechanism for a node rejoining after downtime
- Dial duplicate-avoidance relies on a lexicographic address
  comparison, fragile to inconsistent address formatting across nodes

Validated manually across 3 real spawned nodes: replication converges
(orders placed on different nodes correctly cross-match), leader
election completes and re-elects a new leader with a higher term after
the current leader is killed.