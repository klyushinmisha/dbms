package dbms

/*
TODO: inject logger via interface to prevent logging during benchmarks
*/

import (
	"dbms/pkg/client"
	"dbms/internal/config"
	"dbms/internal/runners"
	"log"
	"testing"
	"time"
)

var dbClient client.ClientCommands

// TestMain runs DBMS server in background before requests execution
// NOTE: placed in this file for ability to run it in tests and benchmarks either
func TestMain(m *testing.M) {
	sr := new(runners.DefaultScopedServerRunner)
	var urlFactory config.ServerUrlFactory = sr
	var runner runners.ScopedServerRunner = sr
	runner.Init()
	defer runner.Finalize()
	// run server in background
	go runner.Run()
	// HACK: wait some time for server to boot
	// TODO: rework
	time.Sleep(time.Second)
	// prepare client
	c, err := client.Connect(urlFactory.BuildUrl())
	if err != nil {
		log.Panic(err)
	}
	defer c.Finalize()
	dbClient = c
	// run tests
	m.Run()
}

// Benchmark_Set tests performance for SET and GET operation;
// Since client works over TCP, network latency impacts on results;
// The idea is to measure overall performance for clients use-cases.
func Benchmark_SetGet(b *testing.B) {
	for n := 0; n < b.N; n++ {
		dbClient.MustSet("key", []byte("some-val"))
		dbClient.MustGet("key")
	}
}

// Benchmark_SetDelete tests performance for SET and DELETE operations;
// Since client works over TCP, network latency impacts on results;
// The idea is to measure overall performance for clients use-cases.
func Benchmark_SetDelete(b *testing.B) {
	for n := 0; n < b.N; n++ {
		dbClient.MustSet("key", []byte("some-val"))
		dbClient.MustDel("key")
	}
}

// Benchmark_TxCommit tests performance for simple committed transaction;
// Since client works over TCP, network latency impacts on results;
// The idea is to measure overall performance for clients use-cases.
func Benchmark_TxCommit(b *testing.B) {
	for n := 0; n < b.N; n++ {
		tx, err := dbClient.BeginEx()
		if err != nil {
			log.Panic(err)
		}
		tx.MustSet("key", []byte("some-val"))
		tx.Commit()
	}
}

// Benchmark_TxAbort tests performance for simple aborted transaction;
// Since client works over TCP, network latency impacts on results;
// The idea is to measure overall performance for clients use-cases.
func Benchmark_TxAbort(b *testing.B) {
	for n := 0; n < b.N; n++ {
		tx, err := dbClient.BeginEx()
		if err != nil {
			log.Panic(err)
		}
		tx.MustSet("key", []byte("some-val"))
		tx.Abort()
	}
}