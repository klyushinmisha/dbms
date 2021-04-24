package recovery

import (
	"dbms/pkg/utils"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
)

func TestRecoveryManager_LogRecovery(t *testing.T) {
	execErr := utils.FileScopedExec("somefile.bin", func(dataFile *os.File) error {
		pageSize := 16
		recMgr := NewRecoveryManager(dataFile, pageSize)
		keys := 32
		for i := 0; i < keys; i++ {
			snapshot := make([]byte, pageSize, pageSize)
			snapshot[0] = byte(i)
			recMgr.LogSnapshot(i, 0, snapshot)
			recMgr.LogCommit(i)
		}
		recMgr.Flush()
		recMgr.Recover()
		return nil
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}
