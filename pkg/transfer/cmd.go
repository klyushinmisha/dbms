package transfer

type Args struct {
	Key   string
	Value []byte
}

type Cmd struct {
	Type int
	Args
}
