package server

import (
	"dbms/pkg/core/transaction"
)

type BeginCommand struct {
	tx    *transaction.Tx
	txMgr *transaction.TxManager
	mode  int
}

func NewBeginCommand(tx *transaction.Tx, txMgr *transaction.TxManager, mode int) *BeginCommand {
	c := new(BeginCommand)
	c.tx = tx
	c.txMgr = txMgr
	c.mode = mode
	return c
}

func (c *BeginCommand) Execute() *Result {
	res := new(Result)
	res.tx = c.tx
	if c.tx != nil {
		res.err = ErrTxStarted
	} else {
		res.tx = c.txMgr.InitTx(c.mode)
	}
	return res
}
