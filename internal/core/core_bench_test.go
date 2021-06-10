package core

import (
	"testing"
	"os"
	"sync"
	"strconv"
	"dbms/internal/config"
	"dbms/internal/core/concurrency"
	"dbms/internal/core/transaction"
	"dbms/internal/core/access/bp_tree"
	bpAdapter "dbms/internal/core/storage/adapters/bp_tree"
)

const workers = 1

func txInsert(tx transaction.Tx, key string, value int64) {
	tree := bp_tree.NewDefaultBPTree(bpAdapter.NewBPTreeAdapter(tx))
	defer tx.Commit()
	tree.Insert(key, int64(1))
}

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
				txInsert(
					coreCfgr.TxMgr().InitTx(concurrency.ExclusiveMode),
					key,
					int64(1),
				)
				wg.Done()
			}(strconv.Itoa(i))
		}
		wg.Wait()
	}
}
