package chunking

import (
	"io"
	"testing"

	"github.com/go-test/deep"
	"github.com/hashicorp/raft"
	"github.com/kr/pretty"
)

type MockFSM struct {
	logs [][]byte
}

func (m *MockFSM) Apply(log *raft.Log) interface{} {
	m.logs = append(m.logs, log.Data)
	return len(m.logs)
}

func (m *MockFSM) Snapshot() (raft.FSMSnapshot, error) {
	return nil, nil
}

func (m *MockFSM) Restore(inp io.ReadCloser) error {
	return nil
}

func TestFSM_Basic(t *testing.T) {
	m := new(MockFSM)
	f := NewChunkingFSM(m)

	data, logs := chunkData(t)

	for i, l := range logs {
		r := f.Apply(&l)
		switch r.(type) {
		case nil:
			if i == len(logs)-1 {
				t.Fatal("expected non-nil value for last log apply")
			}
		case error:
			t.Fatal(r.(error))
		case int:
			if i != len(logs)-1 {
				t.Fatal("got int back before apply should have happened")
			}
			if r.(int) != 1 {
				t.Fatalf("unexpected number of logs back: %d", r.(int))
			}
		default:
			t.Fatal("unexpected return value")
		}
	}

	var finalData []byte
	for _, l := range m.logs {
		finalData = append(finalData, l...)
	}

	if diff := deep.Equal(data, finalData); diff != nil {
		t.Fatal(diff)
	}
}

func TestFSM_ErrorConditions(t *testing.T) {
	data, logs := chunkData(t)

	var m *MockFSM
	var f *ChunkingFSM

	// OpID of zero
	{
		m = new(MockFSM)
		f = NewChunkingFSM(m)

		old := logs[1].ChunkInfo.OpID
		logs[1].ChunkInfo.OpID = 0

		var done bool
		for i, l := range logs {
			r := f.Apply(&l)
			switch r.(type) {
			case nil:
				if i == 1 {
					t.Fatal("expected bad op ID error")
				}
			case error:
				err := r.(error)
				if i != 1 {
					t.Fatalf("unexpected error at value %d: %v; chunk info is %s", i, err, pretty.Sprint(l.ChunkInfo))
				}
				if err != ErrInvalidOpID {
					t.Fatalf("unexpected error: %v", err)
				}
				// Success
				done = true
			default:
				t.Fatal("unexpected return value")
			}

			if done {
				break
			}
		}

		logs[1].ChunkInfo.OpID = old
	}

	// Invalid sequence number
	{
		m = new(MockFSM)
		f = NewChunkingFSM(m)

		old := logs[0].ChunkInfo.SequenceNum
		logs[0].ChunkInfo.SequenceNum = 1

		r := f.Apply(&(logs[0]))
		if r == nil {
			t.Fatal("expected error")
		}
		if r.(error) != ErrNoExistingChunks {
			t.Fatal(r.(error))
		}

		logs[0].ChunkInfo.SequenceNum = old
	}

	// Mismatched sequence number, greater than or less than number of chunks
	{
		seqNumReplacement := func(seqNum int) {
			m = new(MockFSM)
			f = NewChunkingFSM(m)

			old := logs[1].ChunkInfo.SequenceNum
			logs[1].ChunkInfo.SequenceNum = seqNum

			var done bool
			for i, l := range logs {
				r := f.Apply(&l)
				switch r.(type) {
				case nil:
					if i == 1 {
						t.Fatal("expected missing seqnum error")
					}
				case error:
					err := r.(error)
					if i != 1 {
						t.Fatalf("unexpected error at value %d: %v; chunk info is %s", i, err, pretty.Sprint(l.ChunkInfo))
					}
					if err != ErrSequenceNumberMismatch {
						t.Fatalf("unexpected error: %v", err)
					}
					// Success
					done = true
				default:
					t.Fatal("unexpected return value")
				}

				if done {
					break
				}
			}

			logs[1].ChunkInfo.SequenceNum = old
		}

		seqNumReplacement(3)
		seqNumReplacement(0)
	}

	// Term changes
	{
		m = new(MockFSM)
		f = NewChunkingFSM(m)

		old := logs[1].Term
		logs[1].Term = 5

		var done bool
		for i, l := range logs {
			r := f.Apply(&l)
			switch r.(type) {
			case nil:
				if i == len(logs)-1 {
					t.Fatal("expected term mismatch")
				}
			case error:
				err := r.(error)
				if i != len(logs)-1 {
					t.Fatalf("unexpected error at value %d: %v; chunk info is %s", i, err, pretty.Sprint(l.ChunkInfo))
				}
				if err != ErrTermMismatch {
					t.Fatalf("unexpected error: %v", err)
				}
				// Success
				done = true
			default:
				t.Fatal("unexpected return value")
			}

			if done {
				break
			}
		}

		logs[1].Term = old
	}

	// Mismatched data (NumChunks is incorrect)
	{
		m = new(MockFSM)
		f = NewChunkingFSM(m)

		old := logs[7].ChunkInfo.NumChunks
		logs[7].ChunkInfo.NumChunks = 8

		var done bool
		for i, l := range logs {
			r := f.Apply(&l)
			switch r.(type) {
			case nil:
				if i == len(logs)-1 {
					t.Fatal("expected non-nil value for last log apply")
				}
			case error:
				t.Fatal(r.(error))
			case int:
				if i != 7 {
					t.Fatal("got int back before apply should have happened")
				}
				done = true
			default:
				t.Fatal("unexpected return value")
			}
			if done {
				break
			}
		}

		var finalData []byte
		for _, l := range m.logs {
			finalData = append(finalData, l...)
		}

		if diff := deep.Equal(data, finalData); diff == nil {
			t.Fatal("expected a difference")
		}

		logs[7].ChunkInfo.NumChunks = old
	}
}
