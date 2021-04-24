package logging

import (
	"dbms/pkg/utils"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
)

func TestLogManager_Log(t *testing.T) {
	execErr := utils.FileScopedExec("somefile.bin", func(dataFile *os.File) error {
		pageSize := 16
		logMgr := NewLogManager(dataFile, pageSize)
		keys := 32
		snapshot := make([]byte, pageSize, pageSize)
		for i := 0; i < keys; i++ {
			snapshot[0] = byte(i)
			logMgr.LogSnapshot(i, 0, snapshot)
			logMgr.LogCommit(i)
		}
		logMgr.Flush()
		logsIter := logMgr.Iterator()
		for i := 0; i < keys; i++ {
			r := logsIter()
			assert.Equal(t, r.recType, update)
			assert.Equal(t, r.tx, int64(i))
			assert.Equal(t, r.snapshotData[0], byte(i))
			r = logsIter()
			assert.Equal(t, r.recType, commit)
			assert.Equal(t, r.tx, int64(i))
		}
		return nil
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}
