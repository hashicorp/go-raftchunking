package chunking

import (
	"crypto/rand"
	"io"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/hashicorp/raft"
)

func chunkData(t *testing.T) ([]byte, []raft.Log) {
	data := make([]byte, 6000000)
	n, err := rand.Read(data)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != 6000000 {
		t.Fatalf("expected 6000k bytes to test with, read %d", n)
	}

	logs := make([]raft.Log, 0)
	dur := time.Second

	applyFunc := func(l raft.Log, d time.Duration) raft.ApplyFuture {
		if d != dur {
			t.Fatalf("expected d to be %v, got %v", time.Second, dur)
		}
		logs = append(logs, l)
		return raft.ApplyFuture(nil)
	}

	ChunkingApply(data, dur, applyFunc)

	return data, logs
}

func TestApplyChunking(t *testing.T) {
	data, logs := chunkData(t)

	var opID uint64
	var finalData []byte
	for i, l := range logs {
		if i == 0 {
			opID = l.ChunkInfo.OpID
		}
		if l.ChunkInfo.OpID == 0 || l.ChunkInfo.OpID != opID {
			t.Fatalf("bad opid: %d", l.ChunkInfo.OpID)
		}
		if l.ChunkInfo.SequenceNum != i {
			t.Fatalf("bad seqnum; expected %d, got %d", i, l.ChunkInfo.SequenceNum)
		}
		finalData = append(finalData, l.Data...)
	}

	if diff := deep.Equal(data, finalData); diff != nil {
		t.Fatal(diff)
	}
}
