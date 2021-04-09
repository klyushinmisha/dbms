package storage

/*
import (
	"dbms/pkg/concurrency"
	"io"
	"log"
	"os"
	"sync"
)

// TODO: add HeapPageStorageBuilder (HeapPageStorage has a lot of deps)
// to configure this way:
// NewHeapPageStorageBuilder(file, pageSize).UseLockTable(sharedPageLockTable).UseCache(lruCache).UseFSM(fsm).Build()
type HeapPageStorage struct {
	// fileLock locks all disk operations to prevent race conditions
	// during seeking/writing/reading
	fileLock sync.Mutex
	// file storage for heap pages
	file *os.File

	// sharedPageLockTable used to lock page by its position in file
	// to prevent multiple access to the same page during reads/writes
	// used for both HeapPageStorage and cache.Cache
	sharedPageLockTable *concurrency.LockTable
	// appendLock used when position is unknown before WritePage call;
	// allows to escape race conditions during writeNoLock with position generation
	appendLock sync.Mutex

	// pageSize configures total heap page sizeNoLock (with headers, checksum and etc.)
	pageSize int
}

func (s *HeapPageStorage) Empty() bool {
	return s.Size() == 0
}

func (s *HeapPageStorage) PageSize() int {
	return s.pageSize
}

func NewHeapPageStorage(
	file *os.File,
	pageSize int,
	sharedPageLockTable *concurrency.LockTable,
) *HeapPageStorage {
	var s HeapPageStorage
	s.sharedPageLockTable = sharedPageLockTable
	s.file = file
	s.pageSize = pageSize
	return &s
}

func (s *HeapPageStorage) ReadPageAtPos(pos int64) *HeapPage {
	page := AllocatePage(s.pageSize)
	pageData := make([]byte, s.pageSize)
	if _, seekErr := s.file.Seek(pos, io.SeekStart); seekErr != nil {
		log.Panic(seekErr)
	}
	if _, readErr := s.file.Read(pageData); readErr != nil {
		log.Panic(readErr)
	}
	if unmarshalErr := page.UnmarshalBinary(pageData); unmarshalErr != nil {
		log.Panic(unmarshalErr)
	}
	return page
}

func (s *HeapPageStorage) WritePageAtPos(page *HeapPage, pos int64) {
	data, marshalErr := page.MarshalBinary()
	if marshalErr != nil {
		log.Panic(marshalErr)
	}
	if _, writeErr := s.file.Write(data); writeErr != nil {
		log.Panic(writeErr)
	}
	// durability aspect;
	// ensures all fs caches are flushed on disk
	if syncErr := s.file.Sync(); syncErr != nil {
		log.Panic(syncErr)
	}
}

func (s *HeapPageStorage) WritePage(page *HeapPage) int64 {
	pos := s.FindFirstFit(GetHeapPageCapacity(s.pageSize))
	if pos != -1 {
		return pos
	}
	s.appendLock.Lock()
	defer s.appendLock.Unlock()
	pos = s.Size()
	if s.sharedPageLockTable != nil {
		s.sharedPageLockTable.YieldLock(pos, concurrency.ExclusiveMode)
		s.sharedPageLockTable.Unlock(pos)
	}
	s.WritePageAtPos(page, pos)
	return pos
}

func (s *HeapPageStorage) ReleaseNode(pos int64) {
	s.WritePageAtPos(AllocatePage(s.pageSize), pos)
}

func (s *HeapPageStorage) linearScan(exec func(page *HeapPage, pos int64) bool) {
	pos := int64(0)
	for {
		nextPos := pos + int64(s.pageSize)
		if nextPos >= s.Size() {
			return
		}
		stopScan := func() bool {
			if s.sharedPageLockTable != nil {
				s.sharedPageLockTable.YieldLock(pos, concurrency.ExclusiveMode)
				defer s.sharedPageLockTable.Unlock(pos)
			}
			return exec(s.ReadPageAtPos(pos), pos)
		}()
		if stopScan {
			return
		}
		pos = nextPos
	}
}

func (s *HeapPageStorage) FindFirstFit(requiredSpace int) int64 {
	fitPagePos := int64(-1)
	s.linearScan(func(page *HeapPage, pos int64) bool {
		if page.FreeSpace() >= requiredSpace {
			fitPagePos = pos
			return true
		}
		return false
	})
	return fitPagePos
}
*/
