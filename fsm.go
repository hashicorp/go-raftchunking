package raftchunking

import (
	"io"

	"github.com/golang/protobuf/proto"
	"github.com/hashicorp/errwrap"
	"github.com/hashicorp/go-raftchunking/types"
	"github.com/hashicorp/raft"
	"github.com/mitchellh/copystructure"
)

var _ raft.FSM = (*ChunkingFSM)(nil)
var _ raft.ConfigurationStore = (*ChunkingConfigurationStore)(nil)

// ChunkInfo holds chunk information
type ChunkInfo struct {
	Data []byte
}

// ChunkMap represents a set of data chunks. We use ChunkInfo with Data instead
// of bare []byte in case there is a need to extend this info later.
type ChunkMap map[uint64][]*ChunkInfo

type ChunkingFSM struct {
	underlying raft.FSM
	opMap      ChunkMap
	lastTerm   uint64
}

type ChunkingConfigurationStore struct {
	*ChunkingFSM
	underlyingConfigurationStore raft.ConfigurationStore
}

func NewChunkingFSM(underlying raft.FSM) raft.FSM {
	ret := &ChunkingFSM{
		underlying: underlying,
		opMap:      make(ChunkMap),
	}
	return ret
}

func NewChunkingConfigurationStore(underlying raft.ConfigurationStore) raft.ConfigurationStore {
	ret := &ChunkingConfigurationStore{
		ChunkingFSM: &ChunkingFSM{
			underlying: underlying,
			opMap:      make(ChunkMap),
		},
		underlyingConfigurationStore: underlying,
	}
	return ret
}

// Apply applies the log, handling chunking as needed. The return value will
// either be an error or whatever is returned from the underlying Apply.
func (c *ChunkingFSM) Apply(l *raft.Log) interface{} {
	// Not chunking or wrong type, pass through
	if l.Type != raft.LogCommand || l.Extensions == nil {
		return c.underlying.Apply(l)
	}

	if l.Term != c.lastTerm {
		// Term has changed. A raft library client that was applying chunks
		// should get an error that it's no longer the leader and bail, and
		// then any client of (Consul, Vault, etc.) should then retry the full
		// chunking operation automatically, which will be under a different
		// opnum. So it should be safe in this case to clear the map.
		c.opMap = make(ChunkMap)
		c.lastTerm = l.Term
	}

	// Get chunk info from extensions
	var ci types.ChunkInfo
	if err := proto.Unmarshal(l.Extensions, &ci); err != nil {
		return errwrap.Wrapf("error unmarshaling chunk info: {{err}}", err)
	}
	opNum := ci.OpNum
	seqNum := ci.SequenceNum

	// Look up existing chunks; if not existing, make placeholders in the slice
	chunks, ok := c.opMap[opNum]
	if !ok {
		chunks = make([]*ChunkInfo, ci.NumChunks)
		c.opMap[opNum] = chunks
	}

	// Insert the data
	chunks[seqNum] = &ChunkInfo{Data: l.Data}

	for _, chunk := range chunks {
		// Check for nil, but also check data length in case it ends up
		// unmarshaling weirdly for some reason where it makes a new struct
		// instead of keeping the pointer nil
		if chunk == nil || len(chunk.Data) == 0 {
			// Not done yet, so return
			return nil
		}
	}

	finalData := make([]byte, 0, len(chunks)*raft.SuggestedMaxDataSize)

	for _, chunk := range chunks {
		finalData = append(finalData, chunk.Data...)
	}
	delete(c.opMap, opNum)

	// Use the latest log's values with the final data
	logToApply := &raft.Log{
		Index:      l.Index,
		Term:       l.Term,
		Type:       l.Type,
		Data:       finalData,
		Extensions: ci.NextExtensions,
	}

	return c.Apply(logToApply)
}

func (c *ChunkingFSM) Snapshot() (raft.FSMSnapshot, error) {
	return c.underlying.Snapshot()
}

func (c *ChunkingFSM) Restore(rc io.ReadCloser) error {
	return c.underlying.Restore(rc)
}

// Note: this is used in tests via the Raft package test helper functions, even
// if it's not used in client code
func (c *ChunkingFSM) Underlying() raft.FSM {
	return c.underlying
}

func (c *ChunkingFSM) CurrentState() (ChunkMap, error) {
	snapMap, err := copystructure.Copy(c.opMap)
	if err != nil {
		return nil, err
	}
	return snapMap.(ChunkMap), nil
}

func (c *ChunkingConfigurationStore) StoreConfiguration(index uint64, configuration raft.Configuration) {
	c.underlyingConfigurationStore.StoreConfiguration(index, configuration)
}
