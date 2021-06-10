package runners

import (
	"dbms/internal/config"
	"dbms/internal/core"
	"dbms/internal/server"
	"dbms/internal/core/transaction"
	"fmt"
	"os"
)

type ScopedServerRunner interface {
	Init()
	Run()
	Finalize()
}

type DefaultScopedServerRunner struct {
	cfgLdr config.ConfigLoader
	coreBtstp *core.BootstrapManager
	coreCfgr core.DBMSCoreConfigurator
	srvCfgr server.DBMSServerConfigurator
}

func (r *DefaultScopedServerRunner) Init() {
	r.cfgLdr = new(config.DefaultConfigLoader)
	r.cfgLdr.Load()
	r.coreCfgr = core.NewDefaultDBMSCoreConfigurator(r.cfgLdr.CoreCfg())
	r.coreBtstp = r.coreCfgr.BtstpMgr()
	r.coreBtstp.Init()
	r.srvCfgr = server.NewDefaultDBMSServerConfigurator(r.cfgLdr.SrvCfg(), r.coreCfgr)
}

func (r *DefaultScopedServerRunner) Run() {
	// accept incoming connections and process transactions
	r.srvCfgr.ConnSrv().Run()
}

func (r *DefaultScopedServerRunner) Finalize() {
	r.coreBtstp.Finalize()
	if err := os.Remove(r.cfgLdr.CoreCfg().DataPath()); err != nil {
		panic(err)
	}
	if err := os.RemoveAll(r.cfgLdr.CoreCfg().LogPath()); err != nil {
		panic(err)
	}
}

func (r *DefaultScopedServerRunner) BuildUrl() string {
	return fmt.Sprintf("localhost:%d", r.cfgLdr.SrvCfg().Port)
}

func (r *DefaultScopedServerRunner) TxManager() *transaction.TxManager {
	return r.coreCfgr.TxMgr()
}
