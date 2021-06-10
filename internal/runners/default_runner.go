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
	coreFactory core.DBMSCoreFactory
	srvFactory server.DBMSServerFactory
}

func (r *DefaultScopedServerRunner) Init() {
	r.cfgLdr = new(config.DefaultConfigLoader)
	r.cfgLdr.Load()
	r.coreFactory = core.NewDefaultDBMSCoreFactory(r.cfgLdr.CoreCfg())
	r.coreBtstp = r.coreFactory.BtstpMgr()
	r.coreBtstp.Init()
	r.srvFactory = server.NewDefaultDBMSServerFactory(r.cfgLdr.SrvCfg(), r.coreFactory)
}

func (r *DefaultScopedServerRunner) Run() {
	// accept incoming connections and process transactions
	r.srvFactory.ConnSrv().Run()
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
	return r.coreFactory.TxMgr()
}
