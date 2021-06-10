package logging

import (
	"io"
	"log"
	"os"
	"testing"
)

func TestSegmentManager_Log(t *testing.T) {
	txId := 1
	segMgr := NewSegmentManager("./log_segments", 64)
	defer os.RemoveAll("./log_segments")
	segMgr.LoadSegments()
	defer segMgr.CloseSegments()
	rec := new(LogRecord)
	rec.recType = CommitRecord
	rec.txId = int64(txId)
	data, err := rec.MarshalBinary()
	if err != nil {
		log.Panic(err)
	}
	for i := 0; i < 5; i++ {
		segMgr.Log(txId, data)
	}
	segIter := SegmentIterator{segments: segMgr.segments}
	for seg := segIter.Next(); seg != nil; seg = segIter.Next() {
		logIter := LogIterator{seg}
		for r, err := logIter.Next(); err != io.EOF; r, err = logIter.Next() {
			log.Print(r)
		}
	}
	segMgr.Unpin(txId)
}
