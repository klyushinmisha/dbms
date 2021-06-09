package core

import (
	"testing"
	"os"
	"sync"
	"strconv"
	"dbms/internal/config"
	"dbms/internal/core/concurrency"
	"dbms/internal/core/access/bp_tree"
	bpAdapter "dbms/internal/core/storage/adapters/bp_tree"
)

const workers = 1

func Benchmark_CoreInsert(b *testing.B) {
	// prepare
	cfgLdr := new(config.DefaultConfigLoader)
	cfgLdr.Load()
	coreCfgr := NewDefaultDBMSCoreConfigurator(cfgLdr.CoreCfg())
	coreBtstp := coreCfgr.BtstpMgr()
	coreBtstp.Init()
	defer func() {
		coreBtstp.Finalize()
		if err := os.Remove(cfgLdr.CoreCfg().DataPath()); err != nil {
			panic(err)
		}
		if err := os.RemoveAll(cfgLdr.CoreCfg().LogPath()); err != nil {
			panic(err)
		}
	}()
	// bench
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		var wg sync.WaitGroup
		wg.Add(workers)
		for i := 0; i < workers; i++ {
			go func(key string){
				tx := coreCfgr.TxMgr().InitTx(concurrency.ExclusiveMode)
				tree := bp_tree.NewBPTree(100, bpAdapter.NewBPTreeAdapter(tx))
				tree.Insert(key, int64(1))
				tx.Commit()
				wg.Done()
			}(strconv.Itoa(i))
		}
		wg.Wait()
	}
}
