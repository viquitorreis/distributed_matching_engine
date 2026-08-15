package storage

import "testing"

func TestStorage_SaveAndLoad(t *testing.T) {
	dir := t.TempDir()
	s := &Storage{FilePath: dir + "/raft-state.json"}

	if err := s.Save(7, "localhost:9002"); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	term, votedFor, err := s.Load()
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}
	if term != 7 || votedFor != "localhost:9002" {
		t.Fatalf("got term=%d votedFor=%s, want term=7 votedFor=localhost:9002", term, votedFor)
	}
}
