package server

import (
	"dbms/pkg/core/transaction"
)

type AbortCommand struct {
	tx *transaction.Tx
}

func NewAbortCommand(tx *transaction.Tx) *AbortCommand {
	c := new(AbortCommand)
	c.tx = tx
	return c
}

func (c *AbortCommand) Execute() *Result {
	res := new(Result)
	if c.tx != nil {
		c.tx.Abort()
	}
	return res
}
