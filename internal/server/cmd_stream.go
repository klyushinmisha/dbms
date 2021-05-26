package server

import (
	"bufio"
	"dbms/internal/transfer"
	"io"
	"log"
	"net"
	"strings"
)

type CmdStreamIterator interface {
	Next() (string, bool)
}

type BufioCmdStreamIterator struct {
	readStream *bufio.Reader
	stop       bool
}

func DefaultCmdStreamIterator(input io.Reader) *BufioCmdStreamIterator {
	i := new(BufioCmdStreamIterator)
	i.readStream = bufio.NewReader(input)
	return i
}

func (i *BufioCmdStreamIterator) Next() (string, bool) {
	if i.stop {
		return "", false
	}
	rawCmd, err := i.readStream.ReadString('\n')
	i.stop = err == io.EOF
	if i.stop {
		return "", false
	}
	return strings.TrimSpace(rawCmd), true
}

type rawCmdProcessor func(cmd string) *transfer.Result

type RawCmdStreamProcessor interface {
	Pipe(rawCmdProcessor)
}

type DumbRawCmdStreamProcessor struct {
	input  CmdStreamIterator
	output *bufio.Writer
}

func DumbRawCmdStreamProcessorFromConn(conn net.Conn) *DumbRawCmdStreamProcessor {
	s := new(DumbRawCmdStreamProcessor)
	s.input = DefaultCmdStreamIterator(conn)
	s.output = bufio.NewWriter(conn)
	return s
}

func (s *DumbRawCmdStreamProcessor) Pipe(proc rawCmdProcessor) {
	for {
		rawCmd, hasNext := s.input.Next()
		if !hasNext {
			return
		}
		resp, marshalErr := proc(rawCmd).MarshalBinary()
		if marshalErr != nil {
			log.Panic(marshalErr)
		}
		if _, writeErr := s.output.Write(resp); writeErr != nil {
			log.Panic(writeErr)
		}
		s.output.Flush()
	}
}
