package cluster

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

// ProposalEnvelope bundles a proposal's ID with its operation payload,
// so both travel together as a single WriteProposal message body.
type ProposalEnvelope struct {
	Index uint64
	Term  uint64
	Op    []byte
}

func encodeProposal(index, term uint64, op []byte) ([]byte, error) {
	var buf bytes.Buffer

	if err := gob.NewEncoder(&buf).Encode(ProposalEnvelope{Index: index, Term: term, Op: op}); err != nil {
		return nil, fmt.Errorf("encode proposal: %w", err)
	}

	return buf.Bytes(), nil
}

func decodeProposal(body []byte) (index, term uint64, op []byte, err error) {
	var env ProposalEnvelope

	if err := gob.NewDecoder(bytes.NewReader(body)).Decode(&env); err != nil {
		return 0, 0, nil, fmt.Errorf("decode proposal: %w", err)
	}

	return env.Index, env.Term, env.Op, nil
}
