package parser

import (
	"dbms/internal/transfer"
	"errors"
	"regexp"
)

var (
	ErrInvalidCmdStruct = errors.New("Invalid command")
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
		transfer.GetCmdType:    regexp.MustCompile(`^GET ([^\s]+)$`),
		transfer.SetCmdType:    regexp.MustCompile(`^SET ([^\s]+) ([^\s]+)$`),
		transfer.DelCmdType:    regexp.MustCompile(`^DEL ([^\s]+)$`),
		transfer.BegShCmdType:  regexp.MustCompile(`^BEGIN SHARED$`),
		transfer.BegExCmdType:  regexp.MustCompile(`^BEGIN EXCLUSIVE$`),
		transfer.CommitCmdType: regexp.MustCompile(`^COMMIT$`),
		transfer.AbortCmdType:  regexp.MustCompile(`^ABORT$`),
		transfer.HelpCmdType:   regexp.MustCompile(`^HELP$`),
	}
	p.parseStrategies = map[int]parseStrategy{
		transfer.GetCmdType:    oneArgParseStrategy,
		transfer.SetCmdType:    twoArgsParseStrategy,
		transfer.DelCmdType:    oneArgParseStrategy,
		transfer.BegShCmdType:  noArgsParseStrategy,
		transfer.BegExCmdType:  noArgsParseStrategy,
		transfer.CommitCmdType: noArgsParseStrategy,
		transfer.AbortCmdType:  noArgsParseStrategy,
		transfer.HelpCmdType:   noArgsParseStrategy,
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
