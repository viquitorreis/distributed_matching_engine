package cluster

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"log/slog"
	"raft_orderbook/peer"
)

func (c *Cluster) handleMsgProposal(m peer.InboundMsg) {
	idx, term, op, err := decodeProposal(m.Body) // extrai ID + operação do payload
	if err != nil {
		slog.Error("malformed proposal", "error", err, "from", m.From.Addr)

		return
	}

	if !c.raft.AppendAsFollower(idx, term, op) {
		slog.Warn("rejected proposal from stale term", "index", idx, "term", term, "from", m.From.Addr)
		return // dont ack stale leader
	}

	// receiving side now DOES track it, so it has the operation
	// ready when the commit message arrives later
	c.PendingProposals.Register(idx, op)

	m.From.Send(peer.MsgWriteAck, encodeAckBody(idx)) // ack now references the real id
}

func (c *Cluster) handleMsgWriteAck(m peer.InboundMsg) {
	id := binary.BigEndian.Uint64(m.Body)

	_, exists := c.PendingProposals.RecordVote(id, m.From.Addr)
	if !exists {
		// proposal already commited, timed out or acked
		// something this node never proposed so we ignore, its not an errors
		return
	}

	c.maybeCommit(id)
}

func (c *Cluster) handleMsgRequestVote(m peer.InboundMsg) {
	candidateTerm, candidateID, err := decodeRequestVote(m.Body)
	if err != nil {
		slog.Error("malformed request vote", "error", err, "from", m.From.Addr)

		return
	}

	granted, myTerm := c.raft.HandleRequestVote(candidateTerm, candidateID)
	slog.Info("vote decision", "candidate", candidateID, "term", candidateTerm, "granted", granted)
	m.From.Send(peer.MsgVoteGranted, encodeVoteResponse(myTerm, granted))
}

func (c *Cluster) handleMsgLeaderHeartBeat(m peer.InboundMsg) {
	term, leaderID, err := decodeLeaderHeartbeat(m.Body)
	if err != nil {
		slog.Error("malformed leader heartbeat", "error", err, "from", m.From.Addr)

		return
	}

	myTerm := c.raft.HandleHeartbeat(term, leaderID)
	if myTerm <= term { // accepted (not stale)
		c.resetElectionTimer()
	}
}

func (c *Cluster) handleMsgVoteGranted(m peer.InboundMsg) {
	term, granted, err := decodeVoteResponse(m.Body)
	if err != nil {
		slog.Error("malformed vote response", "error", err, "from", m.From.Addr)

		return
	}

	if !granted {
		if term > c.raft.CurrentTerm() {
			c.raft.StepDown(term) // someone is ahead of this peer, yield
		}

		return
	}

	c.electionMu.Lock()
	c.votesReceived[m.From.Addr] = struct{}{}
	votes := len(c.votesReceived)
	c.electionMu.Unlock()

	if votes >= int(c.raft.QuorumSize()) && !c.raft.IsLeader() {
		c.raft.BecomeLeader()
		slog.Info("became leader", "term", c.raft.CurrentTerm())
		c.broadcastLeaderHeartbeat() // assert authority immediately, don't wait for the next tick
	}
}

func (c *Cluster) handleMsgSnapshotRequest(m peer.InboundMsg) {
	snap, err := c.ob.Snapshot()
	if err != nil {
		slog.Error("failed to snapshot orderbook", "error", err)
		return
	}

	body := SnapshotResponseBody{
		LastIncludedIndex: c.raft.LastLogIndex(),
		Data:              snap,
	}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(body); err != nil {
		slog.Error("failed to encode snapshot response", "error", err)
		return
	}

	m.From.Send(peer.MsgSnapshotResponse, buf.Bytes())
}

func (c *Cluster) handleMsgSnapshotResponse(m peer.InboundMsg) {
	var body SnapshotResponseBody
	if err := gob.NewDecoder(bytes.NewReader(m.Body)).Decode(&body); err != nil {
		slog.Error("failed to decode snapshot response body", "error", err)
		return
	}

	if err := c.ob.Restore(body.Data); err != nil {
		slog.Error("failed to restore snapshot", "error", err)
		return
	}

	c.raft.FastForward(body.LastIncludedIndex)
	slog.Info("caught up via snapshot", "last_included_index", body.LastIncludedIndex)
}
