package server

import (
	"bytes"
	"dbms/pkg/core/concurrency"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	GetCmd    = 0
	SetCmd    = 1
	DelCmd    = 2
	BegShCmd  = 3
	BegExCmd  = 4
	CommitCmd = 5
	AbortCmd  = 6
	HelpCmd   = 7
)

var (
	ErrTxStarted = errors.New("tx is already started")
)

type Args struct {
	key   string
	value []byte
}

type Cmd struct {
	cmdType int
	key     string
	value   []byte
}

func (c *Cmd) Type() int {
	return c.cmdType
}

func (c *Cmd) Args() *Args {
	a := new(Args)
	a.key = c.key
	a.value = c.value
	return a
}

type Result struct {
	value []byte
	err   error
}

func (r *Result) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	valueBytes := []byte{}
	if r.value != nil {
		valueBytes = r.value
	}
	if writeErr := binary.Write(buf, binary.LittleEndian, int32(len(valueBytes))); writeErr != nil {
		return nil, writeErr
	}
	if _, writeErr := buf.Write(valueBytes); writeErr != nil {
		return nil, writeErr
	}
	errBytes := []byte{}
	if r.err != nil {
		errBytes = []byte(fmt.Sprintf("%s", r.err))
	}
	if writeErr := binary.Write(buf, binary.LittleEndian, int32(len(errBytes))); writeErr != nil {
		return nil, writeErr
	}
	if _, writeErr := buf.Write(errBytes); writeErr != nil {
		return nil, writeErr
	}
	if _, writeErr := buf.WriteString("\n"); writeErr != nil {
		return nil, writeErr
	}
	return buf.Bytes(), nil
}

type Command interface {
	Execute() *Result
}

type CommandFactory struct {
	txProxy *TxProxy
}

func NewCommandFactory(txProxy *TxProxy) *CommandFactory {
	f := new(CommandFactory)
	f.txProxy = txProxy
	return f
}

func (f *CommandFactory) Create(cmd *Cmd) Command {
	switch cmd.Type() {
	case BegShCmd:
		return NewBeginCommand(f.txProxy, concurrency.SharedMode)
	case BegExCmd:
		return NewBeginCommand(f.txProxy, concurrency.ExclusiveMode)
	case CommitCmd:
		return NewCommitCommand(f.txProxy)
	case AbortCmd:
		return NewAbortCommand(f.txProxy)
	case HelpCmd:
		return NewHelpCommand()
	default:
		return NewDataManipulationCommand(f.txProxy, cmd)
	}
}
