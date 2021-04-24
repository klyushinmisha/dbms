package logging

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"os"
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
	tx      int64
	// snapshot specific fields
	pos          int64
	snapshotData []byte
}

func (r *LogRecord) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	if writeErr := binary.Write(buf, binary.LittleEndian, r.recType); writeErr != nil {
		return nil, writeErr
	}
	if writeErr := binary.Write(buf, binary.LittleEndian, r.tx); writeErr != nil {
		return nil, writeErr
	}
	if r.recType != update {
		return buf.Bytes(), nil
	}
	// log snapshot specific fields
	if writeErr := binary.Write(buf, binary.LittleEndian, r.pos); writeErr != nil {
		return nil, writeErr
	}
	if _, writeErr := buf.Write(r.snapshotData); writeErr != nil {
		return nil, writeErr
	}
	return buf.Bytes(), nil
}

// TODO: add 'flushed' records; skip tx processing if already flushed
// 1. roll forward to get flushed transactions
// 2. roll forward again skipping flushed transactions and processing
//    unflushed transactions
const (
	update = uint8(0)
	commit = uint8(1)
	abort  = uint8(2)
)

type LogManager struct {
	pageSize int
	fileLock sync.Mutex
	file     *os.File
}

func NewLogManager(file *os.File, pageSize int) *LogManager {
	m := new(LogManager)
	m.file = file
	m.pageSize = pageSize
	return m
}

func (m *LogManager) log(r *LogRecord) {
	data, err := r.MarshalBinary()
	if err != nil {
		log.Panic(err)
	}
	m.fileLock.Lock()
	defer m.fileLock.Unlock()
	if _, seekErr := m.file.Seek(0, io.SeekEnd); seekErr != nil {
		log.Panic(seekErr)
	}
	if _, err = m.file.Write(data); err != nil {
		log.Panic(err)
	}
}

func (m *LogManager) LogSnapshot(tx int, pos int64, snapshotData []byte) {
	rec := new(LogRecord)
	rec.recType = update
	rec.tx = int64(tx)
	rec.pos = int64(pos)
	rec.snapshotData = snapshotData
	m.log(rec)
}

func (m *LogManager) LogCommit(tx int) {
	rec := new(LogRecord)
	rec.recType = commit
	rec.tx = int64(tx)
	m.log(rec)
}

func (m *LogManager) LogAbort(tx int) {
	rec := new(LogRecord)
	rec.recType = abort
	rec.tx = int64(tx)
	m.log(rec)
}

func (m *LogManager) Flush() {
	m.fileLock.Lock()
	defer m.fileLock.Unlock()
	// durability aspect;
	// ensures all fs caches are flushed on disk
	if err := m.file.Sync(); err != nil {
		log.Panic(err)
	}
}

func (m *LogManager) Iterator() func() *LogRecord {
	m.fileLock.Lock()
	if _, seekErr := m.file.Seek(0, io.SeekStart); seekErr != nil {
		log.Panic(seekErr)
	}
	stopIter := false
	return func() *LogRecord {
		if stopIter || m.size() == 0 {
			return nil
		}
		rec := m.read()
		if rec == nil {
			stopIter = true
			m.fileLock.Unlock()
		}
		return rec
	}
}

func (m *LogManager) read() *LogRecord {
	r := new(LogRecord)
	if readErr := binary.Read(m.file, binary.LittleEndian, &r.recType); readErr != nil {
		if readErr == io.EOF {
			return nil
		}
		log.Panic(readErr)
	}
	if readErr := binary.Read(m.file, binary.LittleEndian, &r.tx); readErr != nil {
		log.Panic(readErr)
	}
	if r.recType != update {
		return r
	}
	// extract snapshot specific fields
	if readErr := binary.Read(m.file, binary.LittleEndian, &r.pos); readErr != nil {
		log.Panic(readErr)
	}
	r.snapshotData = make([]byte, m.pageSize, m.pageSize)
	if _, err := m.file.Read(r.snapshotData); err != nil {
		log.Panic(err)
	}
	return r
}

func (m *LogManager) size() int64 {
	info, statErr := m.file.Stat()
	if statErr != nil {
		log.Panicln(statErr)
	}
	return info.Size()
}
