package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type Storage struct {
	FilePath string
}

func NewStorage(peerAddr string) *Storage {
	// ':' is invalid in filenames on some platforms
	safe := strings.ReplaceAll(peerAddr, ":", "-")
	return &Storage{
		FilePath: fmt.Sprintf("raft-state-%s.json", safe),
	}
}

type persistedState struct {
	CurrentTerm uint64 `json:"current_term"`
	VotedFor    string `json:"voted_for"`
}

func (s *Storage) Save(term uint64, votedFor string) error {
	data, err := json.Marshal(persistedState{
		CurrentTerm: term,
		VotedFor:    votedFor,
	})
	if err != nil {
		return fmt.Errorf("marshal raft state: %w", err)
	}

	// write to a temp file, then rename an atomic swap, so a crash
	// mid-write never leaves a half-written, corrupt state file behind
	tmp := s.FilePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write raft state: %w", err)
	}

	return os.Rename(tmp, s.FilePath)
}

func (s *Storage) Load() (term uint64, votedFor string, err error) {
	data, err := os.ReadFile(s.FilePath)
	if os.IsNotExist(err) || len(data) == 0 {
		return 0, "", nil // not an error just fresh start
	}

	if err != nil {
		return 0, "", fmt.Errorf("read raft state: %w", err)
	}

	var ps persistedState
	if err := json.Unmarshal(data, &ps); err != nil {
		return 0, "", fmt.Errorf("unmarshal raft state: %w", err)
	}

	return ps.CurrentTerm, ps.VotedFor, nil
}
