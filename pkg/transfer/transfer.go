package transfer

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type Result struct {
	value []byte
	err   error
}

func OkResult() *Result {
	return new(Result)
}

func ValueResult(value []byte) *Result {
	r := new(Result)
	r.value = value
	return r
}

func ErrResult(err error) *Result {
	r := new(Result)
	r.err = err
	return r
}

func (r *Result) Value() []byte {
	return r.value
}

func (r *Result) Err() error {
	return r.err
}

func (r *Result) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	valueBytes := []byte{}
	if r.value != nil {
		valueBytes = r.value
	}
	if writeErr := binary.Write(buf, binary.LittleEndian, int32(len(valueBytes))); writeErr != nil {
		return nil, writeErr
	}
	if _, writeErr := buf.Write(valueBytes); writeErr != nil {
		return nil, writeErr
	}
	errBytes := []byte{}
	if r.err != nil {
		errBytes = []byte(fmt.Sprintf("%s", r.err))
	}
	if writeErr := binary.Write(buf, binary.LittleEndian, int32(len(errBytes))); writeErr != nil {
		return nil, writeErr
	}
	if _, writeErr := buf.Write(errBytes); writeErr != nil {
		return nil, writeErr
	}
	if _, writeErr := buf.WriteString("\n"); writeErr != nil {
		return nil, writeErr
	}
	return buf.Bytes(), nil
}
