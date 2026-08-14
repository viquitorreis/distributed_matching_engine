package raft

import (
	"sync"
)

// Raft is pure state machine, doesn't know nothing about the network, socket or the orderbook itself.
// It only knows: term, state (Follower/Candidate/Leader), who voted in who,
// and the rules of transition between states
type Raft struct {
	state         NodeState
	currentTerm   uint64
	votedFor      string // resets on each new term
	leaderID      string // who this nodes recognizes as the actual leader, "" if doesnt know
	totalNodes    uint64 // cluster size (including this own node), to calculate quorum
	logs          []*LogEntry
	lastLoggedIdx uint64 // separate field, because its cheaper than calculate len(logs) each time

	mu sync.RWMutex
}

func NewRaft(totalNodes uint64) *Raft {
	return &Raft{
		state:      Follower, // always start as follower
		totalNodes: totalNodes,
	}
}

// QuorumSize is the single source of trutrh for how many votes or acks constitute
// the majority, used both for winning an election and later for commiting a replicated
// log entry.
func (r *Raft) QuorumSize() uint64 {
	return (r.totalNodes / 2) + 1
}

// HandleRequestVote decides whether to grant a vote. Returns the
// responders own term too, so the caller (candidate) can detect if its actually
// behing and step down instead of trusting a stale term.
func (r *Raft) HandleRequestVote(candidateTerm uint64, candidateID string) (granted bool, myTerm uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if candidateTerm > r.currentTerm {
		r.currentTerm = candidateTerm
		r.votedFor = ""
		r.state = Follower
	}

	if candidateTerm < r.currentTerm {
		return false, r.currentTerm
	}

	if r.votedFor == "" || r.votedFor == candidateID {
		r.votedFor = candidateID
		return true, r.currentTerm
	}

	return false, r.currentTerm
}

// HandleHeartbeat processes a heartbeat from a claimed leader. A term
// greater than or equal to ours means we recognize them, reset to
// Follower, and the called should reset the election timout
func (r *Raft) HandleHeartbeat(leaderTerm uint64, leaderID string) (myTerm uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if leaderTerm < r.currentTerm {
		return r.currentTerm // stale leader, ignore, dont reset our timeout
	}

	r.currentTerm = leaderTerm
	r.state = Follower
	r.leaderID = leaderID
	r.votedFor = "" // new term via this leader, but we track votedFor per term already above

	return r.currentTerm
}

// BecomeCandidate transitions this node into candidate state, bumps its
// own term, and votes for itsel returning the new term so the caller (cluster)
// knows what to put in the RequestVote broadcast
func (r *Raft) BecomeCandidate(selfID string) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.currentTerm++
	r.state = Candidate
	r.votedFor = selfID

	return r.currentTerm
}

func (r *Raft) IsLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.state == Leader
}

// BecomeLeader transitions Candidate -> Leader. Caller is reponsible
// for having already confirmed votes >= QuorumSize() before calling this.
func (r *Raft) BecomeLeader() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.state = Leader
}

func (r *Raft) CurrentTerm() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.currentTerm
}

// StepDown is called when a Candidate discovers a higher term than its
// own (e.g. a vote response reveals someone is already further ahead).
// It must become a Follower in that higher term.
func (r *Raft) StepDown(term uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if term > r.currentTerm {
		r.currentTerm = term
		r.votedFor = ""
	}

	r.state = Follower
}

// AppendAsLeader append logs guaranteeing that only leaders will do
func (r *Raft) AppendAsLeader(d []byte) (index, term uint64, ok bool) {
	if !r.IsLeader() {
		return 0, 0, false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	idx := r.lastLoggedIdx + 1 // logs start at index 1 on raft, not 0
	entry := &LogEntry{
		Index: idx,
		Term:  r.currentTerm,
		Data:  d,
	}
	r.logs = append(r.logs, entry)
	r.lastLoggedIdx = idx

	return idx, entry.Term, true
}

func (r *Raft) GetLastLoggedIdx() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.lastLoggedIdx
}

// AppendAsFollower records a log entry replicated from the current
// leader. Unlike AppendAsLeader, index and term come from the sender
// this node doesn't generate them, only validates and stores.
func (r *Raft) AppendAsFollower(index, term uint64, data []byte) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if term < r.currentTerm {
		// state leader trying to replicate an old term, reject
		return false
	}

	if term > r.currentTerm {
		r.currentTerm = term
		r.votedFor = ""
		r.state = Follower
	}

	// Today: trust the leaders index directly, no gap/consistency
	// check yet (real Raft verifies the previous entry matches before
	// accepting that's the AppendEntries consistency check, future
	// work).
	r.logs = append(r.logs, &LogEntry{Index: index, Term: term, Data: data})
	if index > r.lastLoggedIdx {
		r.lastLoggedIdx = index
	}

	return true
}
