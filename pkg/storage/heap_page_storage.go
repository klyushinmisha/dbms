package storage

import (
	"dbms/pkg/cache"
	"io"
	"log"
	"os"
)

type HeapPageStorage struct {
	file     *os.File
	fsm      *FSM
	cache    cache.Cache
	pageSize int
	// virtualSize is a size of storage in case of cache usage
	// (when real file size and storage size may differ)
	virtualSize int64
}

func (s *HeapPageStorage) Empty() bool {
	info, statErr := s.file.Stat()
	if statErr != nil {
		log.Panicln(statErr)
	}
	return info.Size() == 0
}

func (s *HeapPageStorage) PageSize() int {
	return s.pageSize
}

// NewHeapPageStorage is constructor for HeapPageStorage. If nil PageCache is passed, the cache will be ignored
func NewHeapPageStorage(
	file *os.File,
	pageSize int,
	cache cache.Cache,
	fsm *FSM,
) *HeapPageStorage {
	return &HeapPageStorage{
		file:     file,
		fsm:      fsm,
		cache:    cache,
		pageSize: pageSize,
	}
}

func (s *HeapPageStorage) Finalize() {
	s.cache.PruneAll(func(pos int64, page interface{}) {
		s.writePageOnDisk(page.(*HeapPage), pos)
	})
}

/*func (s *HeapPageStorage) effectiveFragmentSize() int {
	return s.pageSize / 4
}*/

func (s *HeapPageStorage) readPageFromDisk(pos int64) *HeapPage {
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
	if s.cache != nil {
		if page, found := s.cache.Get(pos); found {
			return page.(*HeapPage)
		}
	}
	page := s.readPageFromDisk(pos)
	if s.cache != nil {
		if prunedPos, prunedPage := s.cache.Put(pos, page); prunedPos != -1 {
			s.writePageOnDisk(prunedPage.(*HeapPage), prunedPos)
		}
	}
	return page
}

func (s *HeapPageStorage) WritePageAtPos(page *HeapPage, pos int64) {
	if s.cache != nil {
		if pos >= s.virtualSize {
			s.virtualSize = pos + int64(s.pageSize)
		}
		if prunedPos, prunedPage := s.cache.Put(pos, page); prunedPos != -1 {
			s.writePageOnDisk(prunedPage.(*HeapPage), prunedPos)
		}
	} else {
		s.writePageOnDisk(page, pos)
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
		info, statErr := s.file.Stat()
		if statErr != nil {
			log.Panicln(statErr)
		}
		pos = info.Size()
	}
	s.WritePageAtPos(page, pos)
	return pos
}
