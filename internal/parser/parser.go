package parser

import (
	"dbms/internal/transfer"
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

type Parser interface {
	Validate(string) error
	Parse(string) (*transfer.Cmd, error)
}

type parseStrategy func(int, []string) *transfer.Cmd

func noArgsParseStrategy(cmdType int, _ []string) *transfer.Cmd {
	cmd := new(transfer.Cmd)
	cmd.Type = cmdType
	return cmd
}

func oneArgParseStrategy(cmdType int, args []string) *transfer.Cmd {
	cmd := new(transfer.Cmd)
	cmd.Type = cmdType
	cmd.Key = args[0]
	return cmd
}

func twoArgsParseStrategy(cmdType int, args []string) *transfer.Cmd {
	cmd := new(transfer.Cmd)
	cmd.Type = cmdType
	cmd.Key = args[0]
	cmd.Value = []byte(args[1])
	return cmd
}

type DumbSingleLineParser struct {
	patterns        map[int]*regexp.Regexp
	parseStrategies map[int]parseStrategy
}

func NewDumbSingleLineParser() *DumbSingleLineParser {
	p := new(DumbSingleLineParser)
	p.patterns = map[int]*regexp.Regexp{
		GetCmd:    regexp.MustCompile(`^GET ([^\s]+)$`),
		SetCmd:    regexp.MustCompile(`^SET ([^\s]+) ([^\s]+)$`),
		DelCmd:    regexp.MustCompile(`^DEL ([^\s]+)$`),
		BegShCmd:  regexp.MustCompile(`^BEGIN SHARED$`),
		BegExCmd:  regexp.MustCompile(`^BEGIN EXCLUSIVE$`),
		CommitCmd: regexp.MustCompile(`^COMMIT$`),
		AbortCmd:  regexp.MustCompile(`^ABORT$`),
		HelpCmd:   regexp.MustCompile(`^HELP$`),
	}
	p.parseStrategies = map[int]parseStrategy{
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

func (p *DumbSingleLineParser) Validate(rawCmd string) error {
	_, err := p.Parse(rawCmd)
	return err
}

func (p *DumbSingleLineParser) Parse(rawCmd string) (*transfer.Cmd, error) {
	for cmdType, r := range p.patterns {
		if match := r.FindStringSubmatch(rawCmd); match != nil {
			return p.parseStrategies[cmdType](cmdType, match[1:]), nil
		}
	}
	return nil, ErrInvalidCmdStruct
}
