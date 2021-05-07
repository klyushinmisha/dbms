package main

import (
	"context"
	"dbms/pkg"
	"fmt"
	"golang.org/x/sync/semaphore"
	"log"
	"net"
	"os"
)

func main() {
	cfgLdr := new(pkg.DefaultConfigLoader)
	cfgLdr.Load()

	dataFile, err := os.OpenFile(cfgLdr.CoreCfg().DataPath(), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalln(err)
	}
	defer dataFile.Close()
	logFile, err := os.OpenFile(cfgLdr.CoreCfg().LogPath(), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalln(err)
	}
	defer logFile.Close()

	coreCfgr := pkg.NewDefaultDBMSCoreConfigurator(cfgLdr.CoreCfg(), dataFile, logFile)
	srvCfgr := pkg.NewDefaultDBMSServerConfigurator(coreCfgr)

	// init storage before recovery attempt
	srvCfgr.TxSrv().InitStorage()

	// run recovery from journal
	coreCfgr.RecMgr().RollForward(coreCfgr.TxMgr())

	ln, err := net.Listen(cfgLdr.SrvCfg().TransportProtocol, fmt.Sprintf(":%d", cfgLdr.SrvCfg().Port))
	if err != nil {
		log.Panic(err)
	}

	sem := semaphore.NewWeighted(int64(cfgLdr.SrvCfg().MaxConnections))
	ctx := context.TODO()
	for {
		func() {
			// acquire weighted semaphore to reduce concurrency
			sem.Acquire(ctx, 1)
			conn, err := ln.Accept()
			if err != nil {
				log.Panic(err)
			}
			go func() {
				defer sem.Release(1)
				srvCfgr.ConnSrv().Serve(conn)
			}()
		}()
	}
}
