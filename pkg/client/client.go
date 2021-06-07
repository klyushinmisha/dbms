package client

import (
	"bufio"
	"dbms/internal/parser"
	"dbms/internal/transfer"
	"net"
	"strings"
)

type RawExecutor interface {
	Exec(rawCmd string) (*transfer.Result, error)
	// MustExec(rawCmd string) *transfer.Result
}

type DataCommands interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte) error
	Del(key string) error
	MustGet(key string) []byte
	MustSet(key string, value []byte)
	MustDel(key string)
}

type TxBeginCommands interface {
	BeginSh() (TxCommands, error)
	BeginEx() (TxCommands, error)
	// MustBeginSh() TxCommands
	// MustBeginEx() TxCommands
}

type TxEndCommands interface {
	Commit() error
	Abort() error
	// MustCommit()
	// MustAbort()
}

type ClientCommands interface {
	RawExecutor
	DataCommands
	TxBeginCommands
}

type TxCommands interface {
	DataCommands
	TxEndCommands
}

// implements TxCommands
type Tx struct {
	c *DBMSClient
	DataCommands
	done bool
}

func NewTx(c *DBMSClient) *Tx {
	tx := new(Tx)
	tx.c = c
	// allows to use client's data commands interface
	tx.DataCommands = c
	return tx
}

// TODO: add locking
// implements ClientCommands
type DBMSClient struct {
	tx     *Tx
	conn   net.Conn
	parser parser.Parser
	writer *bufio.Writer
	send   transfer.ObjectWriter
	recv   transfer.ObjectReader
}

func Connect(host string) (*DBMSClient, error) {
	conn, err := net.Dial("tcp", host)
	if err != nil {
		return nil, err
	}
	reader := bufio.NewReader(conn)
	c := new(DBMSClient)
	c.conn = conn
	c.parser = parser.NewDumbSingleLineParser()
	c.writer = bufio.NewWriter(conn)
	c.send = transfer.NewLEObjectWriter(c.writer)
	c.recv = transfer.NewLEObjectReader(reader)
	return c, nil
}

func (c *DBMSClient) Finalize() {
	c.conn.Close()
}

func (c *DBMSClient) execCmd(cmd transfer.Cmd) (*transfer.Result, error) {
	cmdObj := new(transfer.CmdObject)
	cmdObj.FromCmd(cmd)
	if err := c.send.WriteObject(cmdObj); err != nil {
		return nil, err
	}
	c.writer.Flush()
	resObj := new(transfer.ResultObject)
	if err := c.recv.ReadObject(resObj); err != nil {
		return nil, err
	}
	return resObj.ToResult(), nil
}

/*

 else if cmd.Type == parser.HelpCmdType {
		return transfer.ValueResult([]byte(`Commands structure:
  Data manipulation commands:
    GET key         - finds value associated with key
    SET key value   - sets value associated with key
    DEL key         - removes value associated with key
  Transaction management commands:
    BEGIN SHARED    - starts new transaction with per-operation isolation
    BEGIN EXCLUSIVE - starts new transaction with per-transation isolation
    COMMIT          - commits active transaction
    ABORT           - aborts active transaction`),
		), nil
	}

*/

func (c *DBMSClient) Exec(rawCmd string) (*transfer.Result, error) {
	rawCmd = strings.TrimSpace(rawCmd)
	cmd, err := c.parser.Parse(rawCmd)
	if err != nil {
		return nil, err
	}
	return c.execCmd(*cmd)
}

func handleResult(res *transfer.Result, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}
	if !res.Ok() {
		return nil, res
	}
	return res.Value(), nil
}

func handleMustResult(res *transfer.Result, err error) []byte {
	data, err := handleResult(res, err)
	if err != nil {
		panic(err)
	}
	return data
}

// usual data methods
func (c *DBMSClient) Get(key string) ([]byte, error) {
	return handleResult(c.execCmd(transfer.GetCmd(key)))
}

func (c *DBMSClient) Set(key string, value []byte) error {
	_, err := handleResult(c.execCmd(transfer.SetCmd(key, value)))
	return err
}

func (c *DBMSClient) Del(key string) error {
	_, err := handleResult(c.execCmd(transfer.DelCmd(key)))
	return err
}

// must data methods
func (c *DBMSClient) MustGet(key string) []byte {
	return handleMustResult(c.execCmd(transfer.GetCmd(key)))
}

func (c *DBMSClient) MustSet(key string, value []byte) {
	handleMustResult(c.execCmd(transfer.SetCmd(key, value)))
}

func (c *DBMSClient) MustDel(key string) {
	handleMustResult(c.execCmd(transfer.DelCmd(key)))
}

func (c *DBMSClient) BeginSh() (TxCommands, error) {
	res, err := c.execCmd(transfer.BegShCmd())
	if err != nil {
		return nil, err
	}
	if !res.Ok() {
		return nil, res
	}
	c.tx = NewTx(c)
	return c.tx, nil
}

func (c *DBMSClient) BeginEx() (TxCommands, error) {
	res, err := c.execCmd(transfer.BegExCmd())
	if err != nil {
		return nil, err
	}
	if !res.Ok() {
		return nil, res
	}
	c.tx = NewTx(c)
	return c.tx, nil
}

func (tx *Tx) Commit() error {
	res, err := tx.c.execCmd(transfer.CommitCmd())
	if err != nil {
		return err
	}
	if !res.Ok() {
		return res
	}
	tx.c.tx = nil
	return nil
}

func (tx *Tx) Abort() error {
	res, err := tx.c.execCmd(transfer.AbortCmd())
	if err != nil {
		return err
	}
	if !res.Ok() {
		return res
	}
	tx.c.tx = nil
	return nil
}
