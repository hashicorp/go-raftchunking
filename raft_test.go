package raftchunking

import (
	"crypto/rand"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// This is similar to Raft's TestRaft_TripleNode but performs several large
// value writes in a row
func TestRaftStability_Large_Values(t *testing.T) {
	// Var to store one of the node's FSMs
	var someFSM *ChunkingFSM

	fsmFunc := func() raft.FSM {
		ret := NewChunkingFSM(&raft.MockFSM{}, nil)
		if someFSM == nil {
			someFSM = ret
		}
		return ret
	}

	c := raft.MakeClusterCustom(t, &raft.MakeClusterOpts{
		Peers:           3,
		Bootstrap:       true,
		Conf:            raft.DefaultConfig(),
		MakeFSMFunc:     fsmFunc,
		LongstopTimeout: 30 * time.Second,
	})
	defer c.Close()

	// Should be one leader
	c.Followers()
	leader := c.Leader()
	c.EnsureLeader(t, leader.Leader())

	// Generate a number of large blocks of data
	var dataBlocks [][]byte
	for i := 0; i < 50; i++ {
		data := make([]byte, 10000000)
		n, err := rand.Read(data)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		if n != 10000000 {
			t.Fatalf("expected 10000k bytes to test with, read %d", n)
		}
		dataBlocks = append(dataBlocks, data)
	}

	// Write data blocks in goroutines
	var wg sync.WaitGroup
	var numStarted uint32
	for _, data := range dataBlocks {
		// Should be able to apply
		wg.Add(1)
		go func() {
			defer wg.Done()
			atomic.AddUint32(&numStarted, 1)
			for atomic.LoadUint32(&numStarted) != uint32(len(dataBlocks)) {
				time.Sleep(time.Microsecond)
			}
			future := ChunkingApply(data, nil, 5*time.Second, leader.ApplyLog)
			if err := future.Error(); err != nil {
				c.FailNowf("[ERR] err: %v", err)
			}
		}()
	}

	wg.Wait()
	c.EnsureSame(t)

	// Verify that each log matches _some_ chunk of data. We've already
	// verified the FSMs are the same so only need ot look at one
	mfsm := someFSM.Underlying().(*raft.MockFSM)
	for _, log := range mfsm.Logs() {
		var found bool
		for _, data := range dataBlocks {
			if reflect.DeepEqual(log, data) {
				found = true
				break
			}
		}
		if !found {
			t.Fatal("did not find a block")
		}
	}
}
