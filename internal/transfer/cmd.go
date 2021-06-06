package transfer

type Args struct {
	Key   string
	Value []byte
}

type Cmd struct {
	Type int
	Args
}

const (
	GetCmdType    = 0
	SetCmdType    = 1
	DelCmdType    = 2
	BegShCmdType  = 3
	BegExCmdType  = 4
	CommitCmdType = 5
	AbortCmdType  = 6
	HelpCmdType   = 7
)

func GetCmd(key string) Cmd {
	return Cmd{
		Type: GetCmdType,
		Args: Args{
			Key: key,
		},
	}
}

func SetCmd(key string, value []byte) Cmd {
	return Cmd{
		Type: SetCmdType,
		Args: Args{
			Key:   key,
			Value: value,
		},
	}
}

func DelCmd(key string) Cmd {
	return Cmd{
		Type: DelCmdType,
		Args: Args{
			Key: key,
		},
	}
}

func BegShCmd() Cmd {
	return Cmd{
		Type: BegShCmdType,
	}
}

func BegExCmd() Cmd {
	return Cmd{
		Type: BegExCmdType,
	}
}

func CommitCmd() Cmd {
	return Cmd{
		Type: CommitCmdType,
	}
}

func AbortCmd() Cmd {
	return Cmd{
		Type: AbortCmdType,
	}
}

func HelpCmd() Cmd {
	return Cmd{
		Type: HelpCmdType,
	}
}

type cmdBuilder func(string, []byte) Cmd

func noArgsDecorator(f func() Cmd) cmdBuilder {
	return func(_ string, _ []byte) Cmd {
		return f()
	}
}

func keyArgDecorator(f func(string) Cmd) cmdBuilder {
	return func(key string, _ []byte) Cmd {
		return f(key)
	}
}

var cmdMap = map[int]cmdBuilder{
	GetCmdType:    keyArgDecorator(GetCmd),
	SetCmdType:    SetCmd,
	DelCmdType:    keyArgDecorator(DelCmd),
	BegShCmdType:  noArgsDecorator(BegShCmd),
	BegExCmdType:  noArgsDecorator(BegExCmd),
	CommitCmdType: noArgsDecorator(CommitCmd),
	AbortCmdType:  noArgsDecorator(AbortCmd),
	HelpCmdType:   noArgsDecorator(HelpCmd),
}

func CmdFactory(cmdType int) cmdBuilder {
	return cmdMap[cmdType]
}
