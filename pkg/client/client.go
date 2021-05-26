package client

import (
	"bufio"
	"strings"
	"dbms/internal/parser"
	"net"
	"dbms/internal/transfer"
	"errors"
	"fmt"
)

type RawExecutor interface {
	Exec(rawCmd string) (*transfer.Result, error)
}

type DataCommands interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte) error
	Del(key string) error
}

type TxBeginCommands interface {
	BeginSh() (TxCommands, error)
	BeginEx() (TxCommands, error)
}

type TxCommands interface {
	DataCommands
	Commit() error
	Abort() error
}

type Tx struct {
	c *DBMSClient
	done bool
}

// TODO: add locking
type DBMSClient struct {
	tx *Tx
	conn net.Conn
	pars parser.Parser
	reader *bufio.Reader
	writer *bufio.Writer
}

func Connect(host string) (*DBMSClient, error) {
	conn, err := net.Dial("tcp", host)
	if err != nil {
		return nil, err
	}
	c := new(DBMSClient)
	c.conn = conn
	c.pars = parser.NewDumbSingleLineParser()
	c.reader = bufio.NewReader(conn)
	c.writer = bufio.NewWriter(conn)
	return c, nil
}

func (c *DBMSClient) Finalize() {
	c.conn.Close()
}

func (c *DBMSClient) Exec(rawCmd string) (*transfer.Result, error) {
	rawCmd = strings.TrimSpace(rawCmd)
	if cmd, err := c.pars.Parse(rawCmd); err != nil {
		return nil, err
	} else if cmd.Type == parser.HelpCmd {
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
	c.writer.WriteString(rawCmd)
	c.writer.WriteString("\n")
	c.writer.Flush()
	bytesRes, err := c.reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	var res transfer.Result
	if err := res.UnmarshalBinary(bytesRes); err != nil {
		return nil, err
	}
	return &res, nil
}

func (c *DBMSClient) execFacadeCmd(rawCmd string) ([]byte, error) {
	res, err := c.Exec(rawCmd)
	if err != nil {
		return nil, err
	}
	if !res.Ok() {
		return nil, errors.New(res.Error())
	}
	return res.Value(), nil
}

func (c *DBMSClient) Get(key string) ([]byte, error) {
	return c.execFacadeCmd(fmt.Sprintf("GET %s", key))
}

func (c *DBMSClient) Set(key string, value []byte) error {
	_, err := c.execFacadeCmd(fmt.Sprintf("SET %s %s", key, string(value)))
	return err
}

func (c *DBMSClient) Del(key string) error {
	_, err := c.execFacadeCmd(fmt.Sprintf("DEL %s", key))
	return err
}

func (c *DBMSClient) begin(mode string) (TxCommands, error) {
	if c.tx != nil {
		panic("tx already started")
	}
	_, err := c.execFacadeCmd(fmt.Sprintf("BEGIN %s", mode))
	tx := new(Tx)
	tx.c = c
	c.tx = tx
	return tx, err
}

func (c *DBMSClient) BeginSh() (TxCommands, error) {
	return c.begin("SHARED")
}

func (c *DBMSClient) BeginEx() (TxCommands, error) {
	return c.begin("EXCLUSIVE")
}

func (tx *Tx) Get(key string) ([]byte, error) {
	return tx.c.Get(key)
}

func (tx *Tx) Set(key string, value []byte) error {
	return tx.c.Set(key, value)
}

func (tx *Tx) Del(key string) error {
	return tx.c.Del(key)
}

func (tx *Tx) Commit() error {
	_, err := tx.c.execFacadeCmd("COMMIT")
	tx.c.tx = nil
	return err
}

func (tx *Tx) Abort() error {
	_, err := tx.c.execFacadeCmd("ABORT")
	tx.c.tx = nil
	return err
}
