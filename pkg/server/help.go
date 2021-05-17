package server

import (
	"dbms/pkg/core/transaction"
)

type HelpCommand struct {
	tx *transaction.Tx
}

func NewHelpCommand(tx *transaction.Tx) *HelpCommand {
	return new(HelpCommand)
}

func (c *HelpCommand) Execute() *Result {
	res := new(Result)
	res.tx = c.tx
	res.value = []byte(`Commands structure:
Data manipulation commands:
	GET key         - finds value associated with key
	SET key value   - sets value associated with key
	DEL key         - removes value associated with key
Transaction management commands:
	BEGIN SHARED    - starts new transaction with per-operation isolation
	BEGIN EXCLUSIVE - starts new transaction with per-transation isolation
	COMMIT          - commits active transaction
	ABORT           - aborts active transaction
`)
	return res
}
