package storage

import (
	"log"
)

// DumbPageManager only reads and writes pages
// not transaction-safe
type DumbPageManager struct {
	strgMgr  *StorageManager
	pageSize int
}

func (s *DumbPageManager) ReadPageAtPos(pos int64) *HeapPage {
	page := AllocatePage(s.pageSize)
	block := make([]byte, s.pageSize)
	s.strgMgr.ReadBlock(pos, block)
	if unmarshalErr := page.UnmarshalBinary(block); unmarshalErr != nil {
		log.Panic(unmarshalErr)
	}
	return page
}

func (s *DumbPageManager) WritePageAtPos(page *HeapPage, pos int64) {
	block, marshalErr := page.MarshalBinary()
	if marshalErr != nil {
		log.Panic(marshalErr)
	}
	s.strgMgr.WriteBlock(pos, block)
}

func (s *DumbPageManager) WritePage(page *HeapPage) int64 {
	pos := s.findFirstFit(GetHeapPageCapacity(s.pageSize))
	if pos != -1 {
		s.WritePageAtPos(page, pos)
		return pos
	} else {
		block, marshalErr := page.MarshalBinary()
		if marshalErr != nil {
			log.Panic(marshalErr)
		}
		return s.strgMgr.Extend(block)
	}
}

func (s *DumbPageManager) ClearPage(pos int64) {
	s.WritePageAtPos(AllocatePage(s.pageSize), pos)
}

func (s *DumbPageManager) findFirstFit(requiredSpace int) int64 {
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

func (s *DumbPageManager) linearScan(exec func(page *HeapPage, pos int64) bool) {
	pos := int64(0)
	for {
		nextPos := pos + int64(s.pageSize)
		if nextPos >= s.strgMgr.Size() {
			return
		}
		stopScan := func() bool {
			return exec(s.ReadPageAtPos(pos), pos)
		}()
		if stopScan {
			return
		}
		pos = nextPos
	}
}
