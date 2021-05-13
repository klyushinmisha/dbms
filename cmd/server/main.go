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

	dataFile, err := os.OpenFile(cfgLdr.CoreCfg().DataPath(), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Panic(err)
	}
	defer dataFile.Close()
	logFile, err := os.OpenFile(cfgLdr.CoreCfg().LogPath(), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Panic(err)
	}
	defer logFile.Close()

	coreCfgr := core.NewDefaultDBMSCoreConfigurator(cfgLdr.CoreCfg(), dataFile, logFile)
	srvCfgr := server.NewDefaultDBMSServerConfigurator(cfgLdr.SrvCfg(), coreCfgr)

	log.Print(serverSplash)
	// init storage before recovery attempt
	srvCfgr.TxSrv().InitStorage()
	log.Printf("Initialized storage %s", cfgLdr.CoreCfg().DataPath())
	// run recovery from journal
	coreCfgr.RecMgr().RollForward(coreCfgr.TxMgr())
	log.Printf("Recovered from journal %s", cfgLdr.CoreCfg().LogPath())
	// accept incoming connections and process transactions
	srvCfgr.ConnSrv().Run()
}
