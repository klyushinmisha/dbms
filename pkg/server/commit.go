package server

import (
	"dbms/pkg/core/transaction"
)

type CommitCommand struct {
	tx *transaction.Tx
}

func NewCommitCommand(tx *transaction.Tx) *CommitCommand {
	c := new(CommitCommand)
	c.tx = tx
	return c
}

func (c *CommitCommand) Execute() *Result {
	res := new(Result)
	if c.tx != nil {
		c.tx.Commit()
	}
	return res
}
