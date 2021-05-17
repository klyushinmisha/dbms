package server

type HelpCommand struct {
	res *Result
}

func NewHelpCommand() *HelpCommand {
	c := new(HelpCommand)
	c.res = new(Result)
	return c
}

func (c *HelpCommand) Execute() *Result {
	c.res.value = []byte(`Commands structure:
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
	return c.res
}
