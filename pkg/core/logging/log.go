package logging

import (
	"bytes"
	"encoding/binary"
	"log"
	"sync"
)

// uint8 + int64 + int64
const snapshotRecMetaSize = 17

// uint8 + int64
const commitAbortRecSize = 9

// NOTE: not using lsn, before/after images here;
// use only dumb page snapshots processing to simplify implementation;
// log can be large (stores whole page's snapshot instead of segment)
// but implementation is relatively easy;
// implementation by-design relies on fact that with no-steal strategy for whole
// page's snapshots dumb REDO logging and roll forward recovery is sufficient;
// so only roll forward pages snapshots until stable storage is in required state
type LogRecord struct {
	recType uint8
	txId    int64
	// snapshot specific fields
	Pos         int64
	snapshotLen int64
	Snapshot    []byte
}

func (r *LogRecord) TxId() int {
	return int(r.txId)
}

func (r *LogRecord) Type() int {
	return int(r.recType)
}

func (r *LogRecord) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	if writeErr := binary.Write(buf, binary.LittleEndian, r.recType); writeErr != nil {
		return nil, writeErr
	}
	if writeErr := binary.Write(buf, binary.LittleEndian, r.txId); writeErr != nil {
		return nil, writeErr
	}
	if r.recType != UpdateRecord {
		return buf.Bytes(), nil
	}
	// log snapshot specific fields
	if writeErr := binary.Write(buf, binary.LittleEndian, r.Pos); writeErr != nil {
		return nil, writeErr
	}
	if writeErr := binary.Write(buf, binary.LittleEndian, r.snapshotLen); writeErr != nil {
		return nil, writeErr
	}
	if _, writeErr := buf.Write(r.Snapshot); writeErr != nil {
		return nil, writeErr
	}
	return buf.Bytes(), nil
}

type SegmentIterator struct {
	segments []*Segment
	curIdx   int
}

func (i *SegmentIterator) Next() *Segment {
	if i.curIdx == len(i.segments) {
		return nil
	}
	seg := i.segments[i.curIdx]
	i.curIdx++
	return seg
}

// TODO: add 'flushed' records; skip txId processing if already flushed
// 1. roll forward to get flushed transactions
// 2. roll forward again skipping flushed transactions and processing
//    unflushed transactions
const (
	UpdateRecord = 0
	CommitRecord = 1
	AbortRecord  = 2
)

type LogManager struct {
	logLock sync.Mutex
	segMgr  *SegmentManager
}

func NewLogManager(segMgr *SegmentManager) *LogManager {
	m := new(LogManager)
	m.segMgr = segMgr
	return m
}

func (m *LogManager) log(txId int, r *LogRecord) {
	m.logLock.Lock()
	defer m.logLock.Unlock()
	data, err := r.MarshalBinary()
	if err != nil {
		log.Panic(err)
	}
	m.segMgr.Log(txId, data)
}

func (m *LogManager) LogSnapshot(txId int, pos int64, snapshotData []byte) {
	rec := new(LogRecord)
	rec.recType = UpdateRecord
	rec.txId = int64(txId)
	rec.Pos = pos
	rec.snapshotLen = int64(len(snapshotData))
	rec.Snapshot = snapshotData
	m.log(txId, rec)
}

func (m *LogManager) LogCommit(txId int) {
	rec := new(LogRecord)
	rec.recType = CommitRecord
	rec.txId = int64(txId)
	m.log(txId, rec)
}

func (m *LogManager) LogAbort(txId int) {
	rec := new(LogRecord)
	rec.recType = AbortRecord
	rec.txId = int64(txId)
	m.log(txId, rec)
}

func (m *LogManager) Flush() {
	m.logLock.Lock()
	defer m.logLock.Unlock()
	m.segMgr.Flush()
}

func (m *LogManager) Release(txId int) {
	m.logLock.Lock()
	defer m.logLock.Unlock()
	m.segMgr.Unpin(txId)
}

func (m *LogManager) SegmentIterator() *SegmentIterator {
	i := new(SegmentIterator)
	i.segments = m.segMgr.segments
	return i
}
