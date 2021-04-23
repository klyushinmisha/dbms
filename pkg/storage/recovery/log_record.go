package recovery

import (
	"os"
	"sync"
)

// uint8 + int64 + int64 + int64
const snapshotRecMetaSize = 25

// uint8 + int64
const commitAbortRecSize = 9

type LogRecord struct {
	recType uint8
	tx      int64
	// snapshot specific fields
	pos          int64
	lsn          int64
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
	if r.recType != snapshot {
		return buf.Bytes(), nil
	}
	// log snapshot specific fields
	if writeErr := binary.Write(buf, binary.LittleEndian, r.pos); writeErr != nil {
		return nil, writeErr
	}
	if writeErr := binary.Write(buf, binary.LittleEndian, r.lsn); writeErr != nil {
		return nil, writeErr
	}
	if writeErr := buf.Write(r.snapshotData); writeErr != nil {
		return nil, writeErr
	}
	return buf.Bytes(), nil
}

const (
	snapshot = uint8(0)
	commit   = uint8(1)
	abort    = uint8(2)
)

type RecoveryManager struct {
	pageSize int
	fileLock sync.Mutex
	file     *os.File
}

func (m *RecoveryManager) SnapshotRecSize() int {
	return snapshotRecMetaSize + m.pageSize
}

func (m *RecoveryManager) CommitAbortRecSize() int {
	return commitAbortRecSize
}

func (m *RecoveryManager) log(r *LogRecord) {
	data, err := rec.MarshalBinary()
	if err != nil {
		log.Panic(err)
	}
	m.fileLock.Lock()
	defer m.fileLock.Unlock()
	err = m.file.Write(data)
	if err != nil {
		log.Panic(err)
	}
}

func (m *RecoveryManager) LogSnapshot(tx int, pos int, lsn int, snapshotData []byte) {
	rec := new(LogRecord)
	rec.recType = snapshot
	rec.tx = int64(tx)
	rec.pos = int64(pos)
	rec.lsn = int64(lsn)
	rec.snapshotData = snapshotData
	m.log(rec)
}

func (m *RecoveryManager) LogCommit(tx int) {
	rec := new(LogRecord)
	rec.recType = commit
	rec.tx = int64(tx)
	m.log(rec)
}

func (m *RecoveryManager) LogAbort(tx int) {
	rec := new(LogRecord)
	rec.recType = abort
	rec.tx = int64(tx)
	m.log(rec)
}

func (m *RecoveryManager) readLogRecord() *LogRecord {
	r := new(LogRecord)
	if readErr := binary.Read(m.file, binary.LittleEndian, &r.recType); readErr != nil {
		log.Panic(readErr)
	}
	if readErr := binary.Read(m.file, binary.LittleEndian, &r.tx); readErr != nil {
		log.Panic(readErr)
	}
	if r.recType != snapshot {
		return
	}
	// extract snapshot specific fields
	if readErr := binary.Read(m.file, binary.LittleEndian, &r.pos); readErr != nil {
		log.Panic(readErr)
	}
	if readErr := binary.Read(m.file, binary.LittleEndian, &r.lsn); readErr != nil {
		log.Panic(readErr)
	}
	snapshotBuf = make([]byte, m.pageSize, m.pageSize)
	r.snapshotData = reader.Read(snapshotBuf)
	return r
}

func (m *RecoveryManager) Recovery() {
	m.fileLock.Lock()
	defer m.fileLock.Unlock()
	if _, seekErr := m.file.Seek(0, io.SeekStart); seekErr != nil {
		log.Panic(seekErr)
	}
	// TODO: complete
	r := m.readLogRecord()
	log.Print(r)
}
