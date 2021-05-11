package main

import (
	"dbms/pkg/config"
	"dbms/pkg/core"
	"dbms/pkg/server"
	"log"
	"os"
)

const serverSplash = `

__/\\\\\\\\\\\\_____/\\\\\\\\\\\\\____/\\\\____________/\\\\_____/\\\\\\\\\\\___        
 _\/\\\////////\\\__\/\\\/////////\\\_\/\\\\\\________/\\\\\\___/\\\/////////\\\_       
  _\/\\\______\//\\\_\/\\\_______\/\\\_\/\\\//\\\____/\\\//\\\__\//\\\______\///__      
   _\/\\\_______\/\\\_\/\\\\\\\\\\\\\\__\/\\\\///\\\/\\\/_\/\\\___\////\\\_________     
    _\/\\\_______\/\\\_\/\\\/////////\\\_\/\\\__\///\\\/___\/\\\______\////\\\______    
     _\/\\\_______\/\\\_\/\\\_______\/\\\_\/\\\____\///_____\/\\\_________\////\\\___   
      _\/\\\_______/\\\__\/\\\_______\/\\\_\/\\\_____________\/\\\__/\\\______\//\\\__  
       _\/\\\\\\\\\\\\/___\/\\\\\\\\\\\\\/__\/\\\_____________\/\\\_\///\\\\\\\\\\\/___ 
        _\////////////_____\/////////////____\///______________\///____\///////////_____

                    DBMS - key-value database management system server


`

func main() {
	cfgLdr := new(config.DefaultConfigLoader)
	cfgLdr.Load()

	bootstrapMgr.Init()
	defer bootstrapMgr.Finalize()
	srvCfgr := server.NewDefaultDBMSServerConfigurator(cfgLdr.SrvCfg(), coreCfgr)

	log.Print(serverSplash)
	// accept incoming connections and process transactions
	srvCfgr.ConnSrv().Run()
}
