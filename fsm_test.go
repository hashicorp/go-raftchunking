package raftchunking

import (
	"io"
	"testing"

	"github.com/go-test/deep"
	"github.com/hashicorp/raft"
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
	f := NewChunkingFSM(m, nil)

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
		case ChunkingSuccess:
			if i != len(logs)-1 {
				t.Fatal("got int back before apply should have happened")
			}
			if r.(ChunkingSuccess).Response.(int) != 1 {
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

func TestFSM_StateHandling(t *testing.T) {
	m := new(MockFSM)
	f := NewChunkingFSM(m, nil)

	data, logs := chunkData(t)

	for i, l := range logs {
		if i == len(logs)-1 {
			break
		}
		r := f.Apply(&l)
		switch r.(type) {
		case nil:
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

	var opCount int
	chunks, err := f.store.GetChunks()
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range chunks {
		opCount++
		if opCount > 1 {
			t.Fatalf("unexpected opcount: %d", opCount)
		}
		var validChunks int
		for _, val := range v {
			if val != nil {
				validChunks++
			}
		}
		if validChunks != len(logs)-1 {
			t.Fatalf("unexpected number of chunks: %d; should be one less than len of logs which is %d", validChunks, len(logs))
		}
	}

	currState, err := f.CurrentState()
	if err != nil {
		t.Fatal(err)
	}
	if diff := deep.Equal(chunks, currState.ChunkMap); diff != nil {
		t.Fatal(diff)
	}

	r := f.Apply(&(logs[len(logs)-1]))
	rRaw, ok := r.(ChunkingSuccess)
	if !ok {
		t.Fatalf("wrong type back: %T, value is %#v", r, r)
	}
	rInt, ok := rRaw.Response.(int)
	if !ok {
		t.Fatalf("wrong type back: %T, value is %#v", rRaw, rRaw)
	}
	if rInt != 1 {
		t.Fatalf("unexpected number of logs back: %d", rInt)
	}

	var finalData []byte
	for _, l := range m.logs {
		finalData = append(finalData, l...)
	}

	if diff := deep.Equal(data, finalData); diff != nil {
		t.Fatal(diff)
	}

	newState, err := f.CurrentState()
	if err != nil {
		t.Fatal(err)
	}
	if diff := deep.Equal(chunks, newState.ChunkMap); diff == nil {
		t.Fatal("expected current state to not match chunked state")
	}

	if err := f.RestoreState(currState); err != nil {
		t.Fatal(err)
	}

	newState, err = f.CurrentState()
	if err != nil {
		t.Fatal(err)
	}
	if diff := deep.Equal(chunks, newState.ChunkMap); diff != nil {
		t.Fatal(diff)
	}
}
