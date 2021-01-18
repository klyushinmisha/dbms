package pkg

import (
	"encoding/binary"
	"io"
	"os"
)

type SegmentWriter struct {
	segmentIndex Bitset
	file         *os.File
	pageSize     int64
}

func NewSegmentWriter(file *os.File, segmentIndex Bitset, pageSize int64) *SegmentWriter {
	return &SegmentWriter{
		segmentIndex: segmentIndex,
		file:         file,
		pageSize:     pageSize,
	}
}

// TODO: move to config
var AddrLimit int64 = 1024 * 1024 * 1024
var BytesOrder = binary.LittleEndian

func (v *SegmentWriter) writeSegment(seg *Segment, pos int64) error {
	_, seekErr := v.file.Seek(pos, io.SeekStart)
	if seekErr != nil {
		return seekErr
	}
	writeErr := binary.Write(v.file, BytesOrder, seg.nextSegmentPos)
	if writeErr != nil {
		return writeErr
	}
	writeErr = binary.Write(v.file, BytesOrder, seg.data)
	if writeErr != nil {
		return writeErr
	}
	setErr := v.segmentIndex.Set(pos)
	if setErr != nil {
		return setErr
	}
	return nil
}

func clearBuffer(buf []byte) {
	for i := range buf {
		buf[i] = 0
	}
}

func (v *SegmentWriter) Write(p []byte) (int64, int, error) {
	var firstSegAddr int64 = -1
	var addr int64
	var prevSeg *Segment
	var prevSegAddr int64
	// 8 is size of int64
	var segPayloadLen = v.pageSize - 8
	var bytesToCopy int64
	var buffer = make([]byte, segPayloadLen)

	// TODO: add header to the value
	// | total_size | segment1 | segment2 | ...

	// TODO: move this indexing to the bitset
	// check if bitset byte is eq to 0xFF

	for ; addr < AddrLimit; addr += v.pageSize {
		// check if block in use
		isSet, checkErr := v.segmentIndex.Check(addr)
		if checkErr != nil {
			return 0, 0, checkErr
		}
		if isSet {
			continue
		}
		// now can write at free address
		// but first save address to return from function
		if firstSegAddr == -1 {
			firstSegAddr = addr
		}
		// then if prevSeg exists
		if prevSeg != nil {
			// create link on current block from previous
			// linked list
			prevSeg.nextSegmentPos = addr
			writeErr := v.writeSegment(prevSeg, prevSegAddr)
			if writeErr != nil {
				return 0, 0, writeErr
			}
			clearBuffer(buffer)
		}
		// now create current segment
		bytesToCopy = segPayloadLen
		if int64(len(p)) < bytesToCopy {
			bytesToCopy = int64(len(p))
		}
		copy(buffer[:], p[:bytesToCopy])
		// and move it to prev for use in next iteration
		prevSeg = &Segment{
			nextSegmentPos: -1,
			data:           buffer,
		}
		// also save the address to write
		prevSegAddr = addr
		// advance p
		p = p[bytesToCopy:]
		// if the segment is last, then write it and break
		if len(p) == 0 {
			writeErr := v.writeSegment(prevSeg, prevSegAddr)
			if writeErr != nil {
				return 0, 0, writeErr
			}
			break
		}
	}
	return firstSegAddr, len(p), v.segmentIndex.Flush()
}
