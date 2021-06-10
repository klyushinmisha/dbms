package main

import (
	"dbms/internal/config"
	"dbms/internal/core"
	"dbms/internal/server"
)

func main() {
	cfgLdr := new(config.DefaultConfigLoader)
	cfgLdr.Load()
	coreFactory := core.NewDefaultDBMSCoreFactory(cfgLdr.CoreCfg())
	coreBtstp := coreFactory.BtstpMgr()
	coreBtstp.Init()
	defer coreBtstp.Finalize()
	srvFactory := server.NewDefaultDBMSServerFactory(cfgLdr.SrvCfg(), coreFactory)
	// accept incoming connections and process transactions
	srvFactory.ConnSrv().Run()
}
