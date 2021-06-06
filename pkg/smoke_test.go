package pkg

import (
	"dbms/pkg/client"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
)

func cleanUpKeyBoilerplate(c client.DataCommands, key string) {
	c.MustDel("key")
}

func setAndCheckBoilerplate(c client.DataCommands, key string, expected []byte, t *testing.T) {
	c.MustSet("key", expected)
	actual := c.MustGet("key")
	assert.Equal(t, actual, expected)
}

func getBoilerplate(c client.DataCommands, key string) []byte {
	return c.MustGet("key")
}

// TestDBMS_TxSetCommit is a dumb test if tx commit is applied to store
func TestDBMS_TxSetCommit(t *testing.T) {
	dbClient, err := client.Connect("localhost:8080")
	if err != nil {
		log.Panic(err)
	}
	defer dbClient.Finalize()

	notExpected := []byte("SOME_MAGIC_BUOOOYYY")
	cleanUpKeyBoilerplate(dbClient, "key")
	setAndCheckBoilerplate(dbClient, "key", notExpected, t)

	tx, err := dbClient.BeginEx()
	if err != nil {
		log.Panic(err)
	}
	defer tx.Abort()
	expected := []byte("NEW_MAGIC_BUOOOYYY")
	setAndCheckBoilerplate(tx, "key", expected, t)
	tx.Commit()

	assert.Equal(t, expected, getBoilerplate(dbClient, "key"))
}

// TestDBMS_TxSetCommit is a dumb test if tx abort deallocated all changed data from buffer
func TestDBMS_TxSetAbort(t *testing.T) {
	dbClient, err := client.Connect("localhost:8080")
	if err != nil {
		log.Panic(err)
	}
	defer dbClient.Finalize()

	expected := []byte("SOME_MAGIC_BUOOOYYY")
	cleanUpKeyBoilerplate(dbClient, "key")
	setAndCheckBoilerplate(dbClient, "key", expected, t)

	tx, err := dbClient.BeginEx()
	if err != nil {
		log.Panic(err)
	}
	defer tx.Abort()
	notExpected := []byte("NEW_MAGIC_BUOOOYYY")
	setAndCheckBoilerplate(tx, "key", notExpected, t)
	tx.Abort()

	assert.Equal(t, expected, getBoilerplate(dbClient, "key"))
}
