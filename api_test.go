// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raftchunking

import (
	"crypto/rand"
	"io"
	"testing"
	"time"

	"github.com/go-test/deep"
	"github.com/hashicorp/go-raftchunking/types"
	"github.com/hashicorp/raft"
	proto "google.golang.org/protobuf/proto"
)

func chunkData(t *testing.T) ([]byte, []*raft.Log) {
	data := make([]byte, 6000000)
	n, err := rand.Read(data)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != 6000000 {
		t.Fatalf("expected 6000k bytes to test with, read %d", n)
	}

	logs := make([]*raft.Log, 0)
	dur := time.Second

	var index uint64
	applyFunc := func(l raft.Log, d time.Duration) raft.ApplyFuture {
		if d != dur {
			t.Fatalf("expected d to be %v, got %v", time.Second, dur)
		}
		index++
		l.Index = index
		logs = append(logs, &l)
		return raft.ApplyFuture(nil)
	}

	ChunkingApply(data, nil, dur, applyFunc)

	return data, logs
}

func TestApplyChunking(t *testing.T) {
	data, logs := chunkData(t)

	var opNum uint64
	var finalData []byte
	for i, l := range logs {
		var ci types.ChunkInfo
		if err := proto.Unmarshal(l.Extensions, &ci); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			opNum = ci.OpNum
		}
		if ci.OpNum == 0 || ci.OpNum != opNum {
			t.Fatalf("bad op num: %d", ci.OpNum)
		}
		if ci.SequenceNum != uint32(i) {
			t.Fatalf("bad seqnum; expected %d, got %d", i, ci.SequenceNum)
		}
		finalData = append(finalData, l.Data...)
	}

	if diff := deep.Equal(data, finalData); diff != nil {
		t.Fatal(diff)
	}
}
