package dbms

/*
TODO: inject logger via interface to prevent logging during tests
*/

import (
	"dbms/pkg/client"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
)

// TestDBMS_TxSetCommit is a dumb test if tx commit is applied to store
func TestDBMS_TxSetCommit(t *testing.T) {
	dbClient, err := client.Connect(urlFactory.BuildUrl())
	if err != nil {
		log.Panic(err)
	}
	defer dbClient.Finalize()

	notExpected := []byte("some-val")
	dbClient.Del("key")
	setAndCheckBoilerplate(dbClient, "key", notExpected, t)

	tx, err := dbClient.BeginEx()
	if err != nil {
		log.Panic(err)
	}
	defer tx.Abort()
	expected := []byte("new-magic-val")
	setAndCheckBoilerplate(tx, "key", expected, t)
	tx.Commit()

	assert.Equal(t, expected, dbClient.MustGet("key"))
}

// TestDBMS_TxSetCommit is a dumb test if tx abort deallocated all changed data from buffer
func TestDBMS_TxSetAbort(t *testing.T) {
	dbClient, err := client.Connect(urlFactory.BuildUrl())
	if err != nil {
		log.Panic(err)
	}
	defer dbClient.Finalize()

	expected := []byte("some-val")
	dbClient.Del("key")
	setAndCheckBoilerplate(dbClient, "key", expected, t)

	tx, err := dbClient.BeginEx()
	if err != nil {
		log.Panic(err)
	}
	defer tx.Abort()
	notExpected := []byte("new-magic-val")
	setAndCheckBoilerplate(tx, "key", notExpected, t)
	tx.Abort()

	assert.Equal(t, expected, dbClient.MustGet("key"))
}

func setAndCheckBoilerplate(c client.DataCommands, key string, expected []byte, t *testing.T) {
	c.MustSet("key", expected)
	actual := c.MustGet("key")
	assert.Equal(t, actual, expected)
}
