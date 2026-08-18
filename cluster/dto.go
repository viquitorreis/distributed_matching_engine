package cluster

type RequestVoteBody struct {
	Term        uint64
	CandidateID string
}

type VoteResponseBody struct {
	Term    uint64
	Granted bool
}

type LeaderHeartbeatBody struct {
	Term     uint64
	LeaderID string
}

type SnapshotResponseBody struct {
	LastIncludedIndex uint64
	Data              []byte // orderbook.Snapshot() output
}
