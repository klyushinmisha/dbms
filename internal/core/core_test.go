package core

import (
	"testing"
	"os"
	"sync"
	"strconv"
	"dbms/internal/config"
	"dbms/internal/core/concurrency"
)

func Test_CoreInsert(t *testing.T) {
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
	// test
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
