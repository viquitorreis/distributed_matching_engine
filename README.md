# Distributed Matching Engine

A low-latency order book and matching engine in Go, a multi-node distributed
system with Raft-based leader election, quorum-committed log replication,
snapshot catch-up, and tested failover.

## Local Running

1. Create the cluster

```bash
kind create cluster --name matching-engine
```

2. Build the image

```bash
docker build -t engine:local -f infra/docker/Dockerfile .
```

3. Carry the image to kind

```bash
kind load docker-image engine:local --name matching-engine
```

4. Apply the manifesto:

```bash
kubectl apply -f infra/k8s/engine-statefulset.yaml
```

5. Observe pods getting up

```bash
kubectl get pods -w
```

6. Watch logs:

```bash
kubectl logs engine-0
kubectl logs engine-1
kubectl logs engine-2
```

- Test failover

```bash
kubectl attach -it engine-0
```

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