package chunking

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	proto "github.com/golang/protobuf/proto"
	"github.com/hashicorp/errwrap"
	"github.com/hashicorp/raft"
)

// errorFuture is used to return a static error.
type errorFuture struct {
	err error
}

func (e errorFuture) Error() error {
	return e.err
}

func (e errorFuture) Response() interface{} {
	return nil
}

func (e errorFuture) Index() uint64 {
	return 0
}

type multiFuture struct {
	futures []raft.ApplyFuture
}

func (m multiFuture) Error() error {
	for _, v := range m.futures {
		if err := v.Error(); err != nil {
			return err
		}
	}

	return nil
}

func (m multiFuture) Index() uint64 {
	// This shouldn't happen but need an escape hatch
	if len(m.futures) == 0 {
		return 0
	}

	return m.futures[len(m.futures)-1].Index()
}

func (m multiFuture) Response() interface{} {
	// This shouldn't happen but need an escape hatch
	if len(m.futures) == 0 {
		return nil
	}

	return m.futures[len(m.futures)-1].Response()
}

type ApplyFunc func(raft.Log, time.Duration) raft.ApplyFuture

// ChunkingApply takes in a byte slice and chunks into
// raft.SuggestedMaxDataSize (or less if EOF) chunks, calling Apply on each. It
// requires a corresponding wrapper around the FSM to handle reconstructing on
// the other end. Timeout will be the timeout for each individual operation,
// not total. The return value is a future whose Error() will return only when
// all underlying Apply futures have had Error() return. Note that any error
// indicates that the entire operation will not be applied, assuming the
// correct FSM wrapper is used. If extensions is passed in, it will be set as
// the Extensions value on the Apply once all chunks are received.
func ChunkingApply(cmd, extensions []byte, timeout time.Duration, applyFunc ApplyFunc) raft.ApplyFuture {
	// Create an op ID
	rb := make([]byte, 8)
	n, err := rand.Read(rb)
	if err != nil {
		return errorFuture{err: err}
	}
	if n != 8 {
		return errorFuture{err: fmt.Errorf("expected to read %d bytes for op ID, read %d", 8, n)}
	}
	id := binary.BigEndian.Uint64(rb)

	reader := bytes.NewReader(cmd)

	var logs []raft.Log
	var byteChunks [][]byte
	var mf multiFuture

	remain := reader.Len()
	for {
		if remain <= 0 {
			break
		}

		if remain > raft.SuggestedMaxDataSize {
			remain = raft.SuggestedMaxDataSize
		}

		b := make([]byte, remain)
		n, err := reader.Read(b)
		if err != nil && err != io.EOF {
			return errorFuture{err: err}
		}
		if n != remain {
			return errorFuture{err: fmt.Errorf("expected to read %d bytes from buf, read %d", remain, n)}
		}

		byteChunks = append(byteChunks, b)

		remain = reader.Len()
	}

	for i, chunk := range byteChunks {
		chunkInfo := &ChunkInfo{
			OpNum:       id,
			SequenceNum: uint32(i),
			NumChunks:   uint32(len(byteChunks)),
		}
		if i == len(byteChunks)-1 {
			chunkInfo.NextExtensions = extensions
		}
		chunkBytes, err := proto.Marshal(chunkInfo)
		if err != nil {
			return errorFuture{err: errwrap.Wrapf("error marshaling chunk info: {{err}}", err)}
		}
		logs = append(logs, raft.Log{
			Data:       chunk,
			Extensions: chunkBytes,
		})
	}

	for _, log := range logs {
		mf.futures = append(mf.futures, applyFunc(log, timeout))
	}

	return mf
}
