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
)

var urlFactory config.ServerUrlFactory

// TestMain runs DBMS server in background before requests execution
// NOTE: placed in this file for ability to run it in tests and benchmarks either
func TestMain(m *testing.M) {
	sr := new(runners.DefaultScopedServerRunner)
	urlFactory = sr
	var runner runners.ScopedServerRunner = sr
	runner.Init()
	defer runner.Finalize()
	// run server in background
	go runner.Run()
	// run tests
	m.Run()
}

// Benchmark_Set tests performance for SET command
func Benchmark_Set(b *testing.B) {
	dbClient, err := client.Connect(urlFactory.BuildUrl())
	if err != nil {
		log.Panic(err)
	}
	defer dbClient.Finalize()
	for n := 0; n < b.N; n++ {
		dbClient.MustSet("key", []byte("some-val"))
	}
}

// Benchmark_Set tests performance for DELETE command
func Benchmark_SetAndDelete(b *testing.B) {
	dbClient, err := client.Connect(urlFactory.BuildUrl())
	if err != nil {
		log.Panic(err)
	}
	defer dbClient.Finalize()
	for n := 0; n < b.N; n++ {
		dbClient.MustSet("key", []byte("some-val"))
		dbClient.MustDel("key")
	}
}