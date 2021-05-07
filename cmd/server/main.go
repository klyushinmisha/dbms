package main

import (
	"dbms/pkg"
	"dbms/pkg/concurrency"
	"dbms/pkg/logging"
	"dbms/pkg/recovery"
	"dbms/pkg/storage"
	"dbms/pkg/storage/buffer"
	"dbms/pkg/transaction"
	"log"
	"net"
	"os"
)

const Page8K = 8192

func main() {
	dataFile, err := os.OpenFile("data.bin", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalln(err)
	}
	defer dataFile.Close()
	logFile, err := os.OpenFile("log.bin", os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalln(err)
	}
	defer logFile.Close()

	bufferCap := 8192
	buf := buffer.NewBufferSlotManager(
		storage.NewStorageManager(dataFile, Page8K),
		bufferCap,
		Page8K,
	)
	logMgr := logging.NewLogManager(logFile, Page8K)
	txMgr := transaction.NewTransactionManager(
		0,
		buf,
		logMgr,
		concurrency.NewLockTable(),
	)

	recMgr := recovery.NewRecoveryManager(logMgr)
	recMgr.RollForward(txMgr)

	func() {
		tx := txMgr.InitTx(concurrency.ExclusiveMode)
		defer tx.Commit()
		e := pkg.NewExecutor(tx)
		e.Init()
	}()
	connSrv := pkg.NewConnServer(
		pkg.NewDumbSingleLineParser(),
		pkg.NewTxServer(txMgr),
	)

	// NewRecoveryManager(txMgr, logMgr).RollForward()
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		// handle error
	}

	for {
		// TODO: acquire weighted semaphore to reduce concurrency
		conn, err := ln.Accept()
		if err != nil {
			// handle error
		}
		go connSrv.Serve(conn)
	}
}