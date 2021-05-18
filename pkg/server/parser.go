package server

import (
	"errors"
	"regexp"
)

var (
	ErrInvalidCmdStruct = errors.New("Invalid command")
)

const (
	GetCmd    = 0
	SetCmd    = 1
	DelCmd    = 2
	BegShCmd  = 3
	BegExCmd  = 4
	CommitCmd = 5
	AbortCmd  = 6
	HelpCmd   = 7
)

type Args struct {
	key   string
	value []byte
}

type Cmd struct {
	cmdType int
	key     string
	value   []byte
}

func (c *Cmd) Type() int {
	return c.cmdType
}

func (c *Cmd) Args() *Args {
	a := new(Args)
	a.key = c.key
	a.value = c.value
	return a
}

type Parser interface {
	Parse(string) (*Cmd, error)
}

type DumbSingleLineParser struct {
	patterns        map[int]*regexp.Regexp
	parseStrategies map[int]func(int, []string) *Cmd
}

func noArgsParseStrategy(cmdType int, _ []string) *Cmd {
	cmd := new(Cmd)
	cmd.cmdType = cmdType
	return cmd
}

func oneArgParseStrategy(cmdType int, args []string) *Cmd {
	cmd := new(Cmd)
	cmd.cmdType = cmdType
	cmd.key = args[0]
	return cmd
}

func twoArgsParseStrategy(cmdType int, args []string) *Cmd {
	cmd := new(Cmd)
	cmd.cmdType = cmdType
	cmd.key = args[0]
	cmd.value = []byte(args[1])
	return cmd
}

func NewDumbSingleLineParser() *DumbSingleLineParser {
	p := new(DumbSingleLineParser)
	p.patterns = map[int]*regexp.Regexp{
		GetCmd:    regexp.MustCompile(`^GET ([a-zA-Z0-9_]+)$`),
		SetCmd:    regexp.MustCompile(`^SET ([a-zA-Z0-9_]+) ([a-zA-Z0-9_]+)$`),
		DelCmd:    regexp.MustCompile(`^DEL ([a-zA-Z0-9_]+)$`),
		BegShCmd:  regexp.MustCompile(`^BEGIN SHARED$`),
		BegExCmd:  regexp.MustCompile(`^BEGIN EXCLUSIVE$`),
		CommitCmd: regexp.MustCompile(`^COMMIT$`),
		AbortCmd:  regexp.MustCompile(`^ABORT$`),
		HelpCmd:   regexp.MustCompile(`^HELP$`),
	}
	p.parseStrategies = map[int]func(int, []string) *Cmd{
		GetCmd:    oneArgParseStrategy,
		SetCmd:    twoArgsParseStrategy,
		DelCmd:    oneArgParseStrategy,
		BegShCmd:  noArgsParseStrategy,
		BegExCmd:  noArgsParseStrategy,
		CommitCmd: noArgsParseStrategy,
		AbortCmd:  noArgsParseStrategy,
		HelpCmd:   noArgsParseStrategy,
	}
	return p
}

func (p *DumbSingleLineParser) Parse(strCmd string) (*Cmd, error) {
	for cmdType, r := range p.patterns {
		if match := r.FindStringSubmatch(strCmd); match != nil {
			return p.parseStrategies[cmdType](cmdType, match[1:]), nil
		}
	}
	return nil, ErrInvalidCmdStruct
}
