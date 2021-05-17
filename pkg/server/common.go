package server

import (
	"dbms/pkg/core/concurrency"
	"dbms/pkg/core/transaction"
	"errors"
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
	tx    *transaction.Tx
}

type Command interface {
	Execute() *Result
}

type CommandFactory struct {
	tx    *transaction.Tx
	txMgr *transaction.TxManager
}

func NewCommandFactory(tx *transaction.Tx, txMgr *transaction.TxManager) *CommandFactory {
	f := new(CommandFactory)
	f.tx = tx
	f.txMgr = txMgr
	return f
}

func (f *CommandFactory) Create(cmd *Cmd) Command {
	switch cmd.Type() {
	case BegShCmd:
		return NewBeginCommand(f.tx, f.txMgr, concurrency.SharedMode)
	case BegExCmd:
		return NewBeginCommand(f.tx, f.txMgr, concurrency.ExclusiveMode)
	case CommitCmd:
		return NewCommitCommand(f.tx)
	case AbortCmd:
		return NewAbortCommand(f.tx)
	case HelpCmd:
		return NewHelpCommand(f.tx)
	default:
		return NewDataManipulationCommand(f.tx, f.txMgr, cmd)
	}
}
