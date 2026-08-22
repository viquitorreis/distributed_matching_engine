# Distributed Matching Engine

A low-latency order book and matching engine in Go, evolved from a single-node implementation into a multi-node distributed system with Raft-based leader election, quorum-committed log replication, snapshot catch-up, and tested failover.

## Local Running (Kubernetes via kind)

1. Create the cluster

```bash
kind create cluster --name matching-engine
```

2. Build the image

```bash
docker build -t engine:local -f infra/docker/Dockerfile .
```

3. Load the image into kind

```bash
kind load docker-image engine:local --name matching-engine
```

4. Apply the manifest (swap `image`/`imagePullPolicy` in `infra/k8s/engine-statefulset.yaml` for the local values first, see the comment in the file)

```bash
kubectl apply -f infra/k8s/engine-statefulset.yaml
```

5. Watch pods come up

```bash
kubectl get pods -w
```

6. Watch logs

```bash
kubectl logs engine-0
kubectl logs engine-1
kubectl logs engine-2
```

7. Attach to a pod's CLI to place or cancel orders

```bash
kubectl attach -it engine-0
# commands: order <side:bid|ask> <price> <qty> | cancel <orderID> | list
```

8. Test failover: kill whichever pod's logs show `became leader` most recently, watch a new leader get elected on the survivors with a higher term

```bash
kubectl delete pod engine-0 --grace-period=0 --force
```

A `Makefile` wraps all of the above (`make setup run`, `make restart`, `make logs POD=engine-0`, `make attach-0`, `make kill-leader POD=engine-0`).

## Architecture

Package breakdown, each with a single clear responsibility:

- **orderbook**: matching engine itself (skip list + doubly linked price levels). Wrapped in a thin `Operation`/`Apply` layer so replicated commands (`AddOrder`/`Cancel`) get re-applied deterministically on every node, with `Match()` re-run after each commit to converge to identical state across the cluster. Also exposes `Snapshot`/`Restore` for catch-up.
- **framing**: length-prefix wire protocol (4-byte size + payload), transport-agnostic, shared by every message type below.
- **peer**: owns a single TCP connection end to end. One goroutine for reads, one for writes, so no two goroutines ever touch the same conn concurrently. Identity is established via a blocking Hello handshake before any other message type.
- **cluster**: peer registry and message routing. Once a peer registers, it's asked for a snapshot to catch up. Write proposals go through Raft's leader-only append path (`AppendAsLeader`/`AppendAsFollower`), tracked by log index with a voter set for idempotent acks, and committed to the local order book once quorum is reached, broadcasting `MsgCommit` so every node applies the same command, not just the proposer.
- **raft**: pure consensus state machine, no networking, no knowledge of cluster/peer/orderbook. Implements leader election (Follower/Candidate/Leader states, randomized election timeout, term-based `RequestVote`/`VoteGranted`), an indexed log (`LogEntry{Index, Term, Data}`), and `QuorumSize()` as the single source of truth for majority. Testable in isolation without sockets.
- **storage**: persists `currentTerm`/`votedFor` to disk with atomic temp-file-then-rename writes, loaded back on startup so a restarted node never re-votes in a term it already voted in.
- **cmd/node**: composition root. Wires framing + orderbook + cluster + raft + storage together. Resolves node identity either from CLI args (local dev) or from the pod's own hostname plus `REPLICA_COUNT`/`SERVICE_NAME` env vars (Kubernetes), separating the bind address (what it listens on) from the advertised address (the DNS name or host:port peers use to reach it). Exposes a CLI for placing and cancelling orders.

## Known limitations, documented rather than hidden

- No `prevLogIndex`/`prevLogTerm` consistency check between leader and Follower logs yet. A Follower trusts the leader's index directly, so log conflict detection after a leadership change mid-flight isn't handled.
- No `matchIndex` tracked per Follower on the leader side, replication progress is only visible as an aggregate vote count per proposal, not per peer.
- Read quorum doesn't exist yet, only write quorum. No R+W>N guarantee on reads.
- No client-facing forward-to-leader flow. A Follower rejects `Propose` outright rather than redirecting the caller.
- Cluster membership is static, sized at boot from `REPLICA_COUNT`/peer args. Scaling the Kubernetes `StatefulSet` at runtime won't be reflected in quorum size without a membership-change mechanism, not implemented yet.
- No distributed tracing or latency histograms (p50/p95/p99) on the inter-replica RPC path yet.

## Validated

Across 3 real spawned nodes, both locally and inside a `kind` Kubernetes cluster: replication converges (orders placed on different nodes correctly cross-match), leader election completes and re-elects a new leader with a higher term after the current leader is killed, and a node that reconnects after downtime catches up via snapshot instead of staying permanently behind.
