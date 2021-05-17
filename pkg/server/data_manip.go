package server

import (
	"dbms/pkg/core/concurrency"
	"dbms/pkg/core/transaction"
	"log"
)

type DataManipulationCommand struct {
	tx    *transaction.Tx
	txMgr *transaction.TxManager
	cmd   *Cmd
}

func NewDataManipulationCommand(tx *transaction.Tx, txMgr *transaction.TxManager, cmd *Cmd) *DataManipulationCommand {
	c := new(DataManipulationCommand)
	c.tx = tx
	c.txMgr = txMgr
	c.cmd = cmd
	return c
}

func (c *DataManipulationCommand) runExecutorCommandsInTx(exec func(*Executor), res *Result) {
	defer func() {
		if err := recover(); err == concurrency.ErrTxLockTimeout {
			c.tx.Abort()
			*res = Result{nil, concurrency.ErrTxLockTimeout, nil}
		} else if err != nil {
			log.Panic(err)
		}
	}()
	exec(NewExecutor(c.tx))
}

func (c *DataManipulationCommand) Execute() *Result {
	res := new(Result)
	if c.tx == nil {
		c.tx = c.txMgr.InitTx(concurrency.SharedMode)
		defer c.tx.Commit()
		res.tx = nil
	} else {
		res.tx = c.tx
	}
	c.runExecutorCommandsInTx(func(e *Executor) {
		res = e.ExecuteCmd(c.cmd)
	}, res)
	return res
}
