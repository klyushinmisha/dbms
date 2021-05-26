package main

import (
	"dbms/internal/config"
	"dbms/internal/core"
	"dbms/internal/server"
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
