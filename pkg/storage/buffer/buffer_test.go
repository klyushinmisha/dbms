package buffer

import (
	"dbms/pkg/concurrency"
	"dbms/pkg/storage"
	"dbms/pkg/utils"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"sync"
	"testing"
)

func TestBuffer_FetchFlush(t *testing.T) {
	execErr := utils.FileScopedExec("somefile.bin", func(dataFile *os.File) error {
		strg_mgr := storage.NewStorageManager(dataFile, 8192)
		tab := concurrency.NewLockTable()
		bufferCap := 32
		buf := newBufferSlotManager(strg_mgr, bufferCap, 8192)
		// set keys equal to bufferCap to prevent cache pruning for fetched but not pinned pages
		keys := bufferCap
		threads := 16
		for i := 0; i < keys; i++ {
			pos := int64(i * 8192)
			page := storage.AllocatePage(8192)
			page.AppendData([]byte{byte(i)})
			block, _ := page.MarshalBinary()
			strg_mgr.WriteBlock(pos, block)
		}
		var wg sync.WaitGroup
		wg.Add(threads * keys)
		for i := 0; i < keys; i++ {
			for j := 0; j < threads; j++ {
				go func(blockId int) {
					pos := int64(blockId * 8192)
					tab.YieldLock(pos, concurrency.ExclusiveMode)
					buf.Fetch(pos)
					tab.Unlock(pos)
					// NOTE: item can be evicted
					buf.Pin(pos)
					tab.YieldLock(pos, concurrency.ExclusiveMode)
					page := buf.ReadPageAtPos(pos)
					page.DeleteData(0)
					page.AppendData([]byte{byte(blockId + 1)})
					buf.WritePageAtPos(page, pos)
					tab.Unlock(pos)
					buf.Unpin(pos)
					buf.Flush(pos)
					wg.Done()
				}(i)
			}
		}
		wg.Wait()
		wg.Add(threads * keys)
		for i := 0; i < keys; i++ {
			for j := 0; j < threads; j++ {
				go func(blockId int) {
					pos := int64(blockId * 8192)
					tab.YieldLock(pos, concurrency.ExclusiveMode)
					buf.Fetch(pos)
					tab.Unlock(pos)
					// NOTE: item can be evicted
					buf.Pin(pos)
					tab.YieldLock(pos, concurrency.SharedMode)
					page := buf.ReadPageAtPos(pos)
					assert.Equal(t, page.ReadData(0)[0], byte(blockId+1))
					tab.Unlock(pos)
					buf.Unpin(pos)
					buf.Flush(pos)
					wg.Done()
				}(i)
			}
		}
		wg.Wait()
		return nil
	})
	if execErr != nil {
		log.Panic(execErr)
	}
}
