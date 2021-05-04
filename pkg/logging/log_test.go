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
			assert.Equal(t, r.Type(), UpdateRecord)
			assert.Equal(t, r.TxId(), i)
			assert.Equal(t, r.Snapshot[0], byte(i))
			r = logsIter()
			assert.Equal(t, r.Type(), CommitRecord)
			assert.Equal(t, r.TxId(), i)
		}
		return nil
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}
