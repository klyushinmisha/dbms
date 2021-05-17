package server

type CommitCommand struct {
	txProxy *TxProxy
	res     *Result
}

func NewCommitCommand(txProxy *TxProxy) *CommitCommand {
	c := new(CommitCommand)
	c.txProxy = txProxy
	c.res = new(Result)
	return c
}

func (c *CommitCommand) Execute() *Result {
	c.txProxy.Commit()
	return c.res
}
