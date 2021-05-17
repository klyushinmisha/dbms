package server

type BeginCommand struct {
	txProxy *TxProxy
	mode    int
	res     *Result
}

func NewBeginCommand(txProxy *TxProxy, mode int) *BeginCommand {
	c := new(BeginCommand)
	c.txProxy = txProxy
	c.mode = mode
	c.res = new(Result)
	return c
}

func (c *BeginCommand) Execute() *Result {
	if err := c.txProxy.Init(c.mode); err != nil {
		c.res.err = err
	}
	return c.res
}
