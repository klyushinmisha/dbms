package main

import (
	"dbms/pkg/config"
	"dbms/pkg/core"
	"dbms/pkg/server"
)

func main() {
	cfgLdr := new(config.DefaultConfigLoader)
	cfgLdr.Load()
	coreCfgr := core.NewDefaultDBMSCoreConfigurator(cfgLdr.CoreCfg())
	coreBtstp := coreCfgr.BtstpMgr()
	coreBtstp.Init()
	defer coreBtstp.Finalize()
	srvCfgr := server.NewDefaultDBMSServerConfigurator(cfgLdr.SrvCfg(), coreCfgr)
	// accept incoming connections and process transactions
	srvCfgr.ConnSrv().Run()
}
