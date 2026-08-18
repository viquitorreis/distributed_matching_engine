package cluster

import (
	"context"
	"encoding/binary"
	"log/slog"
	"raft_orderbook/orderbook"
	"raft_orderbook/peer"
	"raft_orderbook/raft"
	"sync"
	"time"
)

type Cluster struct {
	peers map[string]*peer.Peer
	mu    sync.RWMutex

	register   chan *peer.Peer
	unregister chan *peer.Peer

	inbound chan peer.InboundMsg // peer.Peer develivers here what have read from the network
	propose chan []byte          // local ask to propose a write on the cluster

	PendingProposals *raft.PendingProposals
	ownAddr          string // needs to know its own address to create the ID

	ob   *orderbook.OrderBook
	raft *raft.Raft

	electionTimer         *time.Timer
	leaderHeartbeatTicker *time.Ticker
	votesReceived         map[string]struct{}
	electionMu            sync.Mutex
}

func NewCluster(
	ctx context.Context,
	ownAddr string,
	ob *orderbook.OrderBook,
	r *raft.Raft,
) *Cluster {
	cluster := &Cluster{
		peers:                 make(map[string]*peer.Peer),
		register:              make(chan *peer.Peer),
		unregister:            make(chan *peer.Peer),
		inbound:               make(chan peer.InboundMsg),
		propose:               make(chan []byte),
		ownAddr:               ownAddr,
		PendingProposals:      raft.NewPendingProposals(),
		ob:                    ob,
		raft:                  r,
		electionTimer:         time.NewTimer(randomElectionTimeout()),
		leaderHeartbeatTicker: time.NewTicker(leaderHeartbeatEvery),
	}

	go cluster.Bootstrap(ctx)

	return cluster
}

func (c *Cluster) Bootstrap(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case p := <-c.register:
			if c.TryRegister(p.Addr, p) {
				slog.Info("peer registered after handshake", "peer", p.Addr)

				p.Send(peer.MsgSnapshotRequest, nil) // ask the new peer to catch us up with the snapshot
			} else {
				slog.Warn("duplicate connection to already-registered peer, closing", "peer", p.Addr)
				ctx.Done()
			}
		case p := <-c.unregister:
			c.UnregPeer(p)
		case msg := <-c.inbound:
			c.handleInboundMsg(msg)
		case payload := <-c.propose:
			c.broadcastProposal(payload)
		case <-c.electionTimer.C:
			if c.raft.IsLeader() {
				// Leaders don't run election timeouts on themselves just
				// keep the timer harmlessly reset so it's ready if this node
				// ever steps down (e.g. sees a higher-term heartbeat later).
				c.resetElectionTimer()
				continue
			}
			c.startElection()

		case <-c.leaderHeartbeatTicker.C:
			if c.raft.IsLeader() {
				c.broadcastLeaderHeartbeat()
			}
		}
	}
}

func (c *Cluster) TryRegister(identity string, p *peer.Peer) bool {
	c.mu.Lock()

	if _, exists := c.peers[identity]; exists {
		return false
	}

	c.peers[identity] = p
	c.mu.Unlock()

	p.Send(peer.MsgSnapshotRequest, nil) // peer just got registered, need to update from snapshots if its not

	return true
}

func (b *Cluster) UnregPeer(p *peer.Peer) {
	b.mu.Lock()
	delete(b.peers, p.Addr)
	b.mu.Unlock()
}

func (c *Cluster) handleInboundMsg(m peer.InboundMsg) {
	switch m.Type {
	case peer.MsgWriteProposal:
		c.handleMsgProposal(m)

	case peer.MsgWriteAck:

	case peer.MsgCommit:
		id := binary.BigEndian.Uint64(m.Body)
		c.applyCommitted(id) // new: apply locally, same logic maybeCommit used to inline

	case peer.MsgHeartbeat:
		m.From.MarkAlive()

	case peer.MsgRequestVote:
		c.handleMsgRequestVote(m)

	case peer.MsgLeaderHeartbeat:
		c.handleMsgLeaderHeartBeat(m)

	case peer.MsgVoteGranted:
		c.handleMsgVoteGranted(m)

	case peer.MsgSnapshotRequest:
		c.handleMsgSnapshotRequest(m)

	case peer.MsgSnapshotResponse:
		c.handleMsgSnapshotResponse(m)

	case peer.MsgHello:
		// handshake
	}
}

func (c *Cluster) broadcastProposal(payload []byte) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, p := range c.peers {
		p.Send(peer.MsgWriteProposal, payload)
	}
}

func (c *Cluster) InboundChan() chan<- peer.InboundMsg {
	return c.inbound
}

func (c *Cluster) Register(p *peer.Peer) {
	c.register <- p
}

// Propose originates a new write proposal from THIS node. Generates a
// unique ID, tracks it as pending, broadcasts to every peer, and counts this node's
// own implicit vote immediately (it doesnt need to ack itself over the network)
func (c *Cluster) Propose(op []byte) (uint64, bool) {
	logIdx, term, ok := c.raft.AppendAsLeader(op)
	if !ok {
		return 0, false
	}

	c.PendingProposals.Register(logIdx, op)
	// count our own vote right away, quorum counts us as 1 without
	// needing a round-trip ack to ourselves
	c.PendingProposals.RecordVote(logIdx, c.ownAddr)

	payload, err := encodeProposal(logIdx, term, op)
	if err != nil {
		slog.Error("failed to encode proposal", "error", err)
		return 0, false // err signaling??
	}

	c.mu.RLock()
	for _, p := range c.peers {
		p.Send(peer.MsgWriteProposal, payload)
	}
	c.mu.RUnlock()

	c.maybeCommit(logIdx) // in case it reached quorum with only its own vote (cluster with only 1 node)

	return logIdx, true
}

// maybeCommit checks where a proposal has reached quorum, and if so,
// applies it to the local order book and stops tracking it
func (c *Cluster) maybeCommit(entryIdx uint64) {
	c.mu.RLock()
	quorum := (len(c.peers)+1)/2 + 1 // +1 counting this node on total this clusters have
	c.mu.RUnlock()

	// we need to expose this actual vote without incrementing the new one, adjust in
	// PendingProposals: a read only method, like VoteCount(entryIdx)
	votes := c.PendingProposals.VoteCount(entryIdx)
	if votes < quorum {
		return
	}

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, entryIdx)

	// quorum reached broadcast commit so every peer applies too
	c.mu.RLock()
	for _, p := range c.peers {
		p.Send(peer.MsgCommit, buf)
	}
	c.mu.RUnlock()

	c.applyCommitted(entryIdx) // apply on the proposer itself too
}

func (c *Cluster) applyCommitted(id uint64) {
	pp := c.PendingProposals.Get(id)
	if pp == nil {
		return // already applied, or this node never tracked it
	}

	op, err := orderbook.DecodeOperation(pp.Operation)
	if err != nil {
		slog.Error("failed to decode committed operation", "id", id, "error", err)
		c.PendingProposals.Remove(id)
		return
	}

	c.ob.Apply(op)
	trades := c.ob.Match()
	c.PendingProposals.Remove(id)
	slog.Info("proposal committed", "id", id, "trades", len(trades))
}

func (c *Cluster) GetOrders() []orderbook.Order {
	return c.ob.AllOrders()
}

func (c *Cluster) IsLeader() bool {
	return c.raft.IsLeader()
}

func (c *Cluster) CurrentTerm() uint64 {
	return c.raft.CurrentTerm()
}
