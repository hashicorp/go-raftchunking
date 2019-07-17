package middleware

import (
	"errors"
	"io"

	"github.com/hashicorp/raft"
)

var (
	ErrTermMismatch = errors.New("term mismatch during reconstruction of chunks, please resubmit")

	ErrInvalidOpID = errors.New("no op ID found when reconstructing chunks")

	ErrNoExistingChunks = errors.New("no existing chunks but non-zero sequence num")

	ErrSequenceNumberMismatch = errors.New("sequence number skipped")

	ErrMissingChunk = errors.New("missing sequence number during reconstruction")
)

type chunkInfo struct {
	term   uint64
	seqNum int
	data   []byte
}

type ChunkingFSM struct {
	underlying raft.FSM
	opMap      map[uint64][]chunkInfo
}

func NewChunkingFSM(underlying raft.FSM) *ChunkingFSM {
	return &ChunkingFSM{
		underlying: underlying,
		opMap:      make(map[uint64][]chunkInfo),
	}
}

// Apply applies the log, handling chunking as needed. The return value will
// either be an error or whatever is returned from the underlying Apply.
func (c *ChunkingFSM) Apply(l *raft.Log) interface{} {
	// Not chunking, pass through
	if l.ChunkInfo == nil {
		return c.underlying.Apply(l)
	}

	opID := l.ChunkInfo.OpID
	seqNum := l.ChunkInfo.SequenceNum

	if opID == 0 {
		return ErrInvalidOpID
	}

	// Look up existing chunks
	chunks, ok := c.opMap[opID]
	if !ok {
		if seqNum != 0 {
			return ErrNoExistingChunks
		}
	}

	// Do early detection of a loss or other problem
	if seqNum != len(chunks) {
		delete(c.opMap, opID)
		return ErrSequenceNumberMismatch
	}

	chunks = append(chunks, chunkInfo{
		term:   l.Term,
		seqNum: seqNum,
		data:   l.Data,
	})

	if l.ChunkInfo.SequenceNum == l.ChunkInfo.NumChunks-1 {
		// Run through and reconstruct the data
		finalData := make([]byte, 0, len(chunks)*raft.SuggestedMaxDataSize)

		var term uint64
		for i, chunk := range chunks {
			if i == 0 {
				term = chunk.term
			}
			if chunk.seqNum != i {
				return ErrMissingChunk
			}
			if chunk.term != term {
				return ErrTermMismatch
			}
			finalData = append(finalData, chunk.data...)
		}

		// Use the latest log's values with the final data
		logToApply := &raft.Log{
			Index: l.Index,
			Term:  l.Term,
			Type:  l.Type,
			Data:  finalData,
		}

		return c.Apply(logToApply)
	}

	// Otherwise, re-add to map and return
	c.opMap[opID] = chunks
	return nil
}

func (c *ChunkingFSM) Snapshot() (raft.FSMSnapshot, error) {
	return c.underlying.Snapshot()
}

func (c *ChunkingFSM) Restore(rc io.ReadCloser) error {
	return c.underlying.Restore(rc)
}

func (c *ChunkingFSM) Underlying() raft.FSM {
	return c.underlying
}