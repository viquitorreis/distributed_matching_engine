package raft

type LogEntry struct {
	Index uint64
	Term  uint64
	Data  []byte
}
