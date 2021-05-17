package server

type AbortCommand struct {
	txProxy *TxProxy
	res     *Result
}

func NewAbortCommand(txProxy *TxProxy) *AbortCommand {
	c := new(AbortCommand)
	c.txProxy = txProxy
	c.res = new(Result)
	return c
}

func (c *AbortCommand) Execute() *Result {
	c.txProxy.Abort()
	return c.res
}
