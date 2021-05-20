package transfer

import (
	"bytes"
	"encoding/binary"
)

type Result struct {
	ok    bool
	value []byte
	err   string
}

func OkResult() *Result {
	r := new(Result)
	r.ok = true
	return r
}

func ValueResult(value []byte) *Result {
	r := new(Result)
	r.ok = true
	r.value = value
	return r
}

func ErrResult(err error) *Result {
	r := new(Result)
	r.ok = false
	r.err = err.Error()
	return r
}

func (r *Result) Ok() bool {
	return r.ok
}

func (r *Result) Value() []byte {
	return r.value
}

func (r *Result) Err() string {
	return r.err
}

func (r *Result) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	if writeErr := binary.Write(buf, binary.LittleEndian, r.ok); writeErr != nil {
		return nil, writeErr
	}
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
	errBytes := []byte(r.err)
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

func (r *Result) UnmarshalBinary(data []byte) error {
	buf := bytes.NewBuffer(data)
	if readErr := binary.Read(buf, binary.LittleEndian, &r.ok); readErr != nil {
		return readErr
	}
	var valueLen int32
	if readErr := binary.Read(buf, binary.LittleEndian, &valueLen); readErr != nil {
		return readErr
	}
	r.value = buf.Next(int(valueLen))
	var errLen int32
	if readErr := binary.Read(buf, binary.LittleEndian, &errLen); readErr != nil {
		return readErr
	}
	r.err = string(buf.Next(int(errLen)))
	return nil
}
