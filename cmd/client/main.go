package main

/*
type DBMSRepl struct {

}

type DBMSClient struct {
	conn net.Conn
	// NewDumbSingleLineParser()
	parser Parser
	tx *ClientTx
}

func (c *DBMSClient) Exec(cmd string) error {
	// send cmd to conn
}

func (c *DBMSClient) BeginTx() *ClientTx {
	// send cmd to conn
}

const (
	TxStateRunning = 0
	TxStateFinished = 1
)

var (
	ErrTxFinished = errors.New("tx processing finished")
)

type ClientTx struct {
	state int
}

func (tx *ClientTx) Exec(cmd string) *transfer.Result {
	if tx.state == TxStateFinished {
		return transfer.ErrResult(ErrTxFinished)
	}
	if _, parseErr := tx.c.parser.Parse(strings.TrimSpace(cmd)); parseErr != nil {
		return transfer.ErrResult(parseErr)
	}
	return tx.exec(cmd)
}

func (tx *ClientTx) Commit() {
	if tx.state != TxStateFinished {
		tx.commit()
	}
	return transfer.OkResult()
}

func (tx *ClientTx) Abort() {
	if tx.state != TxStateFinished {
		tx.abort()
	}
	return transfer.OkResult()
}

func main() {
	client := NewDBMSClient(host, port)
	res = client.Exec("GET key")
	if res.err != nil {
		...
	}
	tx := client.BeginTx()
	defer tx.Commit()
	tx.Exec("SET key value")
	tx.
}
*/

import (
	"os"
	"bufio"
	"fmt"
	"dbms/pkg/client"
	"log"
	"io"
	"dbms/internal/transfer"
)

// simple REPL for manual tests
func main() {
	dbClient, err := client.Connect("localhost:8080")
	defer dbClient.Finalize()
	if err != nil {
		log.Panic(err)
	}
	reader := bufio.NewReader(os.Stdin)
	for {
		var msg string
		fmt.Print("> ")
		rawStrCmd, _ := reader.ReadString('\n')
		res, err := dbClient.Exec(rawStrCmd)
		if err == io.EOF {
			return
		} else if err != nil {
			log.Panic(err)
		}
		switch res.Type() {
		case transfer.OkResultCode:
			msg = "OK"
			break
		case transfer.ValueResultCode:
			msg = string(res.Value())
			break
		case transfer.ErrResultCode:
			msg = res.Error()
			break
		}
		fmt.Println(msg)
	}
}
