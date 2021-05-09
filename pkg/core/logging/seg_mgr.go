package logging

import (
	"dbms/pkg/atomic"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"
)

// NOTE: pls, dudes, add generics to Golang 2
type IntHashSet struct {
	set map[int]struct{}
}

func NewIntHashSet() *IntHashSet {
	s := new(IntHashSet)
	s.set = make(map[int]struct{})
	return s
}

func (s *IntHashSet) Empty() bool {
	return len(s.set) == 0
}

func (s *IntHashSet) Add(value int) {
	s.set[value] = struct{}{}
}

func (s *IntHashSet) Remove(value int) {
	delete(s.set, value)
}

func (s *IntHashSet) Has(value int) bool {
	_, found := s.set[value]
	return found
}

// NOTE: not thread-safe
type LogIterator struct {
	seg *Segment
}

func (i *LogIterator) Next() (*LogRecord, error) {
	if i.seg.sizeNoLock() == 0 {
		return nil, io.EOF
	}
	return i.seg.readNoLock()
}

type Segment struct {
	id       int
	cap      int
	fileLock sync.Mutex
	file     *os.File
	txRefSet *IntHashSet
}

func NewSegment(id int, cap int, file *os.File) *Segment {
	s := new(Segment)
	s.id = id
	s.cap = cap
	s.file = file
	s.txRefSet = NewIntHashSet()
	if _, seekErr := s.file.Seek(0, io.SeekEnd); seekErr != nil {
		log.Panic(seekErr)
	}
	return s
}

func (s *Segment) Name() string {
	return s.file.Name()
}

func (s *Segment) Id() int {
	return s.id
}

func (s *Segment) Pin(txId int) {
	s.txRefSet.Add(txId)
}

func (s *Segment) Unpin(txId int) {
	s.txRefSet.Remove(txId)
}

func (s *Segment) Used() bool {
	return !s.txRefSet.Empty()
}

func (s *Segment) Append(data []byte) bool {
	if len(data)+s.sizeNoLock() > s.cap {
		return false
	}
	if _, writeErr := s.file.Write(data); writeErr != nil {
		log.Panic(writeErr)
	}
	return true
}

func (s *Segment) Flush() {
	s.fileLock.Lock()
	defer s.fileLock.Unlock()
	// durability aspect;
	// ensures all fs caches are flushed on disk
	if err := s.file.Sync(); err != nil {
		log.Panic(err)
	}
}

func (s *Segment) CloseAndRemoveFile() {
	name := s.file.Name()
	s.Close()
	os.Remove(name)
}

func (s *Segment) Close() {
	s.file.Close()
}

func (s *Segment) LogIterator() *LogIterator {
	i := new(LogIterator)
	i.seg = s
	if _, seekErr := s.file.Seek(0, io.SeekStart); seekErr != nil {
		log.Panic(seekErr)
	}
	return i
}

func (s *Segment) sizeNoLock() int {
	info, statErr := s.file.Stat()
	if statErr != nil {
		log.Panicln(statErr)
	}
	return int(info.Size())
}

func (s *Segment) readNoLock() (*LogRecord, error) {
	r := new(LogRecord)
	if readErr := binary.Read(s.file, binary.LittleEndian, &r.recType); readErr != nil {
		if readErr == io.EOF {
			return nil, io.EOF
		}
		log.Panic(readErr)
	}
	if readErr := binary.Read(s.file, binary.LittleEndian, &r.txId); readErr != nil {
		log.Panic(readErr)
	}
	if r.recType != UpdateRecord {
		return r, nil
	}
	// extract snapshot specific fields
	if readErr := binary.Read(s.file, binary.LittleEndian, &r.Pos); readErr != nil {
		log.Panic(readErr)
	}
	if readErr := binary.Read(s.file, binary.LittleEndian, &r.snapshotLen); readErr != nil {
		log.Panic(readErr)
	}
	r.Snapshot = make([]byte, r.snapshotLen, r.snapshotLen)
	if _, err := s.file.Read(r.Snapshot); err != nil {
		log.Panic(err)
	}
	return r, nil
}

// NOTE: not thread-safe; used only with LogManager in concurrent mode
type SegmentManager struct {
	segDir    string
	segCap    int
	segments  []*Segment
	segIdCtr  atomic.AtomicCounter
	activeSeg *Segment
}

type SegmentsToSort []*Segment

func (s SegmentsToSort) Len() int           { return len(s) }
func (s SegmentsToSort) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s SegmentsToSort) Less(i, j int) bool { return s[i].Id() < s[j].Id() }

func (m *SegmentManager) loadSegments(segDir string, segCap int) {
	logFileRegex := regexp.MustCompile(`^segment([0-9]+)\.bin$`)
	m.segments = make([]*Segment, 0)
	err := filepath.WalkDir(segDir, func(path string, _ fs.DirEntry, err error) error {
		baseName := filepath.Base(path)
		if match := logFileRegex.FindStringSubmatch(baseName); match != nil {
			segId, err := strconv.Atoi(match[1])
			if err != nil {
				return err
			}
			segFile, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
			if err != nil {
				return err
			}
			m.segments = append(m.segments, NewSegment(segId, segCap, segFile))
		}
		return nil
	})
	if err != nil {
		// corrupted log
		log.Panic(err)
	}
	sort.Sort(SegmentsToSort(m.segments))
}

func NewSegmentManager(segDir string, segCap int) *SegmentManager {
	m := new(SegmentManager)
	m.segDir = segDir
	m.segCap = segCap
	return m
}

func (m *SegmentManager) LoadSegments() {
	if _, err := os.Stat(m.segDir); os.IsNotExist(err) {
		if dirErr := os.Mkdir(m.segDir, 0777); dirErr != nil {
			log.Panic(dirErr)
		}
	}
	m.loadSegments(m.segDir, m.segCap)
	if len(m.segments) == 0 {
		m.allocateNewSegment()
	}
	m.activeSeg = m.segments[len(m.segments)-1]
	m.segIdCtr.Init(m.activeSeg.Id())
}

// Log returns segId in which data was logged to
func (m *SegmentManager) Log(txId int, data []byte) {
	m.activeSeg.Pin(txId)
	if !m.activeSeg.Append(data) {
		m.activeSeg.Flush()
		m.activeSeg = m.allocateNewSegment()
		m.activeSeg.Pin(txId)
		// new segment is expected to fit new record
		m.activeSeg.Append(data)
		m.pruneOldSegments()
	}
}

func (m *SegmentManager) Unpin(txId int) {
	for _, seg := range m.segments {
		seg.Unpin(txId)
	}
	m.pruneOldSegments()
}

func (m *SegmentManager) Flush() {
	m.activeSeg.Flush()
}

func (m *SegmentManager) CloseSegments() {
	for _, seg := range m.segments {
		seg.Close()
	}
}

func (m *SegmentManager) allocateNewSegment() *Segment {
	newSegId := m.segIdCtr.Incr()
	// TODO: remove hardcoded value
	name := filepath.Join(m.segDir, fmt.Sprintf("segment%d.bin", newSegId))
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Panic(err)
	}
	seg := NewSegment(newSegId, m.segCap, file)
	m.segments = append(m.segments, seg)
	return seg
}

func (m *SegmentManager) pruneOldSegments() {
	for segIdx, seg := range m.segments {
		if seg.Used() || seg.Id() == m.activeSeg.Id() {
			m.segments = m.segments[segIdx:]
			return
		}
		seg.CloseAndRemoveFile()
	}
}
