package storage

import (
	"dbms/pkg/cache"
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
	// fsm allows to speed up free page lookups and except linear scans
	fsm *FSM
	// cache stores heap pages before they are pruned and written to disk
	// nil cache means no caching is used
	cache cache.Cache
	// virtualSize is a size of storage in case of cache usage
	// (when real file size and storage size may differ)
	virtualSize int64
	// writeLock used when position is unknown before WritePage call;
	// allows to escape race conditions during write with position generation
	writeLock sync.Mutex

	// pageSize configures total heap page size (with headers, checksum and etc.)
	pageSize int
}

func (s *HeapPageStorage) Empty() bool {
	if s.cache != nil {
		return s.virtualSize == 0
	}
	return s.getRealSize() == 0
}

func (s *HeapPageStorage) PageSize() int {
	return s.pageSize
}

func NewHeapPageStorage(
	file *os.File,
	pageSize int,
	cache cache.Cache,
	sharedPageLockTable *concurrency.LockTable,
	fsm *FSM,
) *HeapPageStorage {
	var s HeapPageStorage
	s.sharedPageLockTable = sharedPageLockTable
	s.file = file
	s.fsm = fsm
	s.cache = cache
	s.pageSize = pageSize
	s.virtualSize = s.getRealSize()
	return &s
}

func (s *HeapPageStorage) Finalize() {
	if s.cache != nil {
		s.cache.PruneAll(func(pos int64, page interface{}) {
			func() {
				if s.sharedPageLockTable != nil {
					defer s.sharedPageLockTable.Unlock(pos)
				}
				s.writePageOnDisk(page.(*HeapPage), pos)
			}()
		})
	}
}

func (s *HeapPageStorage) readPageFromDisk(pos int64) *HeapPage {
	s.fileLock.Lock()
	defer s.fileLock.Unlock()
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

func (s *HeapPageStorage) writePageOnDisk(page *HeapPage, pos int64) {
	s.fileLock.Lock()
	defer s.fileLock.Unlock()
	_, seekErr := s.file.Seek(pos, io.SeekStart)
	if seekErr != nil {
		log.Panic(seekErr)
	}
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

func (s *HeapPageStorage) ReadPageAtPos(pos int64) *HeapPage {
	if s.sharedPageLockTable != nil {
		s.sharedPageLockTable.YieldLock(pos)
		defer s.sharedPageLockTable.Unlock(pos)
	}
	if s.cache != nil {
		if page, found := s.cache.Get(pos); found {
			return page.(*HeapPage)
		}
	}
	page := s.readPageFromDisk(pos)
	if s.cache != nil {
		s.cachePutWithPrune(page, pos)
	}
	return page
}

func (s *HeapPageStorage) WritePageAtPos(page *HeapPage, pos int64) {
	if s.sharedPageLockTable != nil {
		s.sharedPageLockTable.YieldLock(pos)
		defer s.sharedPageLockTable.Unlock(pos)
	}
	if s.cache != nil {
		if pos >= s.virtualSize {
			s.virtualSize = pos + int64(s.pageSize)
		}
		s.cachePutWithPrune(page, pos)
	} else {
		s.writePageOnDisk(page, pos)
	}
}

func (s *HeapPageStorage) cachePutWithPrune(page *HeapPage, pos int64) {
	// cache is expected to return locked position;
	// unlock it after write
	if prunedPos, prunedPage := s.cache.Put(pos, page); prunedPos != -1 {
		// prevents premature release
		if pos != prunedPos {
			if s.sharedPageLockTable != nil {
				defer s.sharedPageLockTable.Unlock(prunedPos)
			}
		}
		s.writePageOnDisk(prunedPage.(*HeapPage), prunedPos)
	}
}

func (s *HeapPageStorage) WritePage(page *HeapPage) int64 {
	// TODO: improve position generation in all cases
	s.writeLock.Lock()
	defer s.writeLock.Unlock()
	var pos int64
	if s.fsm != nil {
		pos = s.fsm.FindFirstFit(255)
	} else {
		pos = s.findFirstFit(GetHeapPageCapacity(s.pageSize))
	}
	if pos == -1 {
		pos = s.Size()
	}
	s.WritePageAtPos(page, pos)
	return pos
}

func (s *HeapPageStorage) Size() int64 {
	if s.cache != nil {
		return s.virtualSize
	}
	return s.getRealSize()
}

func (s *HeapPageStorage) getRealSize() int64 {
	info, statErr := s.file.Stat()
	if statErr != nil {
		log.Panicln(statErr)
	}
	return info.Size()
}

func (s *HeapPageStorage) ReleaseNode(pos int64) {
	// TODO: add locking
	if s.fsm != nil {
		s.fsm.SetLevel(pos, FreePageLevel)
	}
	s.WritePageAtPos(AllocatePage(s.pageSize), pos)
}

func (s *HeapPageStorage) linearScan(exec func(page *HeapPage, pos int64)) {
	pos := int64(0)
	for {
		nextPos := pos + int64(s.pageSize)
		if nextPos >= s.Size() {
			return
		}
		exec(s.ReadPageAtPos(pos), pos)
		pos = nextPos
	}
}

func (s *HeapPageStorage) findFirstFit(requiredSpace int) int64 {
	fitPagePos := int64(-1)
	s.linearScan(func(page *HeapPage, pos int64) {
		if page.FreeSpace() >= requiredSpace {
			fitPagePos = pos
			return
		}
	})
	return fitPagePos
}
