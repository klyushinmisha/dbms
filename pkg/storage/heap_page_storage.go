package storage

import (
	"dbms/pkg/cache"
	"dbms/pkg/concurrency"
	"io"
	"log"
	"os"
	"sync"
)

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
				defer s.sharedPageLockTable.Unlock(pos)
				s.writePageOnDisk(page.(*HeapPage), pos)
			}()
		})
	}
}

/*func (s *HeapPageStorage) effectiveFragmentSize() int {
	return s.pageSize / 4
}*/

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
}

func (s *HeapPageStorage) ReadPageAtPos(pos int64) *HeapPage {
	s.sharedPageLockTable.YieldLock(pos)
	defer s.sharedPageLockTable.Unlock(pos)
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
	s.sharedPageLockTable.YieldLock(pos)
	defer s.sharedPageLockTable.Unlock(pos)
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
			defer s.sharedPageLockTable.Unlock(prunedPos)
		}
		s.writePageOnDisk(prunedPage.(*HeapPage), prunedPos)
	}
}

func (s *HeapPageStorage) WritePage(page *HeapPage) int64 {
	if s.fsm != nil {
		pos := s.fsm.FindFirstFit(255)
		if pos != -1 {
			s.WritePageAtPos(page, pos)
		}
		return pos
	}
	var pos int64
	if s.cache != nil {
		pos = s.virtualSize
	} else {
		pos = s.getRealSize()
	}
	s.WritePageAtPos(page, pos)
	return pos
}

func (s *HeapPageStorage) getRealSize() int64 {
	info, statErr := s.file.Stat()
	if statErr != nil {
		log.Panicln(statErr)
	}
	return info.Size()
}
