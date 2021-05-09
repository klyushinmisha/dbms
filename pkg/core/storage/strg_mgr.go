package storage

import (
	"io"
	"log"
	"os"
	"sync"
)

type StorageManager struct {
	// fileLock locks all disk operations to prevent race conditions
	// during seeking/writing/reading
	fileLock sync.Mutex
	// file storage for heap pages
	file *os.File
	// pageSize configures total heap page size (with headers, checksum and etc.)
	pageSize   int
	emptyBlock []byte
}

func NewStorageManager(
	file *os.File,
	pageSize int,
) *StorageManager {
	var m StorageManager
	m.file = file
	m.pageSize = pageSize
	block, err := AllocatePage(pageSize).MarshalBinary()
	if err != nil {
		log.Panic(err)
	}
	m.emptyBlock = block
	return &m
}

func (m *StorageManager) Empty() bool {
	return m.Size() == 0
}

func (m *StorageManager) PageSize() int {
	return m.pageSize
}

func (m *StorageManager) Size() int64 {
	m.fileLock.Lock()
	defer m.fileLock.Unlock()
	return m.sizeNoLock()
}

func (m *StorageManager) ReadBlock(pos int64, block []byte) {
	m.fileLock.Lock()
	defer m.fileLock.Unlock()
	if _, seekErr := m.file.Seek(pos, io.SeekStart); seekErr != nil {
		log.Panic(seekErr)
	}
	if _, readErr := m.file.Read(block); readErr != nil {
		log.Panic(readErr)
	}
}

func (m *StorageManager) WriteBlock(pos int64, block []byte) {
	m.fileLock.Lock()
	defer m.fileLock.Unlock()
	m.writeNoLock(pos, block)
}

func (m *StorageManager) Extend() int64 {
	m.fileLock.Lock()
	defer m.fileLock.Unlock()
	pos := m.sizeNoLock()
	m.writeNoLock(pos, m.emptyBlock)
	return pos
}

func (m *StorageManager) Flush() {
	// durability aspect;
	// ensures all fs caches are flushed on disk
	if syncErr := m.file.Sync(); syncErr != nil {
		log.Panic(syncErr)
	}
}

func (m *StorageManager) writeNoLock(pos int64, block []byte) {
	_, seekErr := m.file.Seek(pos, io.SeekStart)
	if seekErr != nil {
		log.Panic(seekErr)
	}
	if _, writeErr := m.file.Write(block); writeErr != nil {
		log.Panic(writeErr)
	}
}

func (m *StorageManager) sizeNoLock() int64 {
	info, statErr := m.file.Stat()
	if statErr != nil {
		log.Panicln(statErr)
	}
	return info.Size()
}
