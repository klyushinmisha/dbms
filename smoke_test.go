package dbms

import (
	"dbms/pkg/client"
	"dbms/internal/config"
	"dbms/internal/core"
	"dbms/internal/server"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
	"fmt"
)

var cfgLdr config.ConfigLoader

// buildServerUrl builds server's url for test clients
func buildServerUrl() string {
	return fmt.Sprintf("localhost:%d", cfgLdr.SrvCfg().Port)
}

// TestMain runs DBMS server in background before requests execution
func TestMain(m *testing.M) {
	cfgLdr = new(config.DefaultConfigLoader)
	cfgLdr.Load()
	coreCfgr := core.NewDefaultDBMSCoreConfigurator(cfgLdr.CoreCfg())
	coreBtstp := coreCfgr.BtstpMgr()
	coreBtstp.Init()
	defer coreBtstp.Finalize()
	srvCfgr := server.NewDefaultDBMSServerConfigurator(cfgLdr.SrvCfg(), coreCfgr)
	// run server in background
	go func(){
		// accept incoming connections and process transactions
		srvCfgr.ConnSrv().Run()
	}()
	// run tests
	m.Run()
}

// TestDBMS_TxSetCommit is a dumb test if tx commit is applied to store
func TestDBMS_TxSetCommit(t *testing.T) {
	dbClient, err := client.Connect(buildServerUrl())
	if err != nil {
		log.Panic(err)
	}
	defer dbClient.Finalize()

	notExpected := []byte("SOME_MAGIC_BUOOOYYY")
	dbClient.MustDel("key")
	setAndCheckBoilerplate(dbClient, "key", notExpected, t)

	tx, err := dbClient.BeginEx()
	if err != nil {
		log.Panic(err)
	}
	defer tx.Abort()
	expected := []byte("NEW_MAGIC_BUOOOYYY")
	setAndCheckBoilerplate(tx, "key", expected, t)
	tx.Commit()

	assert.Equal(t, expected, dbClient.MustGet("key"))
}

// TestDBMS_TxSetCommit is a dumb test if tx abort deallocated all changed data from buffer
func TestDBMS_TxSetAbort(t *testing.T) {
	dbClient, err := client.Connect(buildServerUrl())
	if err != nil {
		log.Panic(err)
	}
	defer dbClient.Finalize()

	expected := []byte("SOME_MAGIC_BUOOOYYY")
	dbClient.MustDel("key")
	setAndCheckBoilerplate(dbClient, "key", expected, t)

	tx, err := dbClient.BeginEx()
	if err != nil {
		log.Panic(err)
	}
	defer tx.Abort()
	notExpected := []byte("NEW_MAGIC_BUOOOYYY")
	setAndCheckBoilerplate(tx, "key", notExpected, t)
	tx.Abort()

	assert.Equal(t, expected, dbClient.MustGet("key"))
}

func setAndCheckBoilerplate(c client.DataCommands, key string, expected []byte, t *testing.T) {
	c.MustSet("key", expected)
	actual := c.MustGet("key")
	assert.Equal(t, actual, expected)
}
