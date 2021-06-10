package logging

import (
	"dbms/internal/utils"
	"github.com/stretchr/testify/assert"
	"io"
	"log"
	"os"
	"testing"
)

func TestLogManager_Log(t *testing.T) {
	execErr := utils.FileScopedExec("somefile.bin", func(dataFile *os.File) error {
		defer os.RemoveAll("./log_segments")
		pageSize := 16
		segMgr := NewSegmentManager("./log_segments", 128)
		segMgr.LoadSegments()
		defer segMgr.CloseSegments()
		logMgr := NewLogManager(segMgr)
		keys := 16
		snapshot := make([]byte, pageSize, pageSize)
		for i := 0; i < keys; i++ {
			snapshot[0] = byte(i)
			logMgr.LogSnapshot(i, 0, snapshot)
		}
		logMgr.Flush()
		segIter := SegmentIterator{segments: segMgr.segments}
		i := 0
		for seg := segIter.Next(); seg != nil; seg = segIter.Next() {
			logIter := LogIterator{seg}
			for r, err := logIter.Next(); err != io.EOF; r, err = logIter.Next() {
				assert.Equal(t, r.Type(), UpdateRecord)
				assert.Equal(t, r.TxId(), i)
				assert.Equal(t, r.Snapshot[0], byte(i))
				logMgr.Release(i)
				i++
			}
		}
		return nil
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}
