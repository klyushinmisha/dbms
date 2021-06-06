package transfer

import (
	"bytes"
	"encoding/binary"
	"io"
)

const (
	CmdObjectType    = 0
	ResultObjectType = 1
)

const headerSize = 5

type header struct {
	Type byte
	Size uint32
}

type Object interface {
	Header() header
	Body() []byte
	Create(data []byte)
}

type ObjectReader struct {
	r io.Reader
}

func NewObjectReader(r io.Reader) *ObjectReader {
	or := new(ObjectReader)
	or.r = r
	return or
}

func (or *ObjectReader) ReadObject(obj Object) error {
	var hdr header
	if err := binary.Read(or.r, binary.LittleEndian, &hdr); err != nil {
		return err
	}
	size := int(hdr.Size)
	body := make([]byte, size, size)
	if _, err := or.r.Read(body); err != nil {
		return err
	}
	obj.Create(body)
	return nil
}

type ObjectWriter struct {
	w io.Writer
}

func NewObjectWriter(w io.Writer) *ObjectWriter {
	ow := new(ObjectWriter)
	ow.w = w
	return ow
}

func (ow *ObjectWriter) WriteObject(obj Object) error {
	if err := binary.Write(ow.w, binary.LittleEndian, obj.Header()); err != nil {
		return err
	}
	_, err := ow.w.Write(obj.Body())
	return err
}

func mustDumpValueToBuffer(buf io.Writer, value interface{}) {
	if err := binary.Write(buf, binary.LittleEndian, value); err != nil {
		panic(err)
	}
}

func mustDumpBytesToBuffer(buf io.Writer, data []byte) {
	// write payload len
	mustDumpValueToBuffer(buf, uint32(len(data)))
	// write payload
	if _, err := buf.Write(data); err != nil {
		panic(err)
	}
}

type CmdObject struct {
	cmdType byte
	key     []byte
	value   []byte
}

func (o *CmdObject) Header() header {
	return header{
		byte(CmdObjectType),
		uint32(len(o.Body())),
	}
}

func (o *CmdObject) Body() []byte {
	buf := new(bytes.Buffer)
	mustDumpValueToBuffer(buf, o.cmdType)
	mustDumpBytesToBuffer(buf, o.key)
	mustDumpBytesToBuffer(buf, o.value)
	return buf.Bytes()
}

func mustReadValueFromBuffer(buf io.Reader, ptr interface{}) {
	if err := binary.Read(buf, binary.LittleEndian, ptr); err != nil {
		panic(err)
	}
}

func mustReadBytesFromBuffer(buf io.Reader) []byte {
	var valueSize uint32
	mustReadValueFromBuffer(buf, &valueSize)
	if valueSize == 0 {
		return []byte{}
	}
	data := make([]byte, valueSize, valueSize)
	if _, err := buf.Read(data); err != nil {
		panic(err)
	}
	return data
}

func (o *CmdObject) Create(data []byte) {
	buf := bytes.NewReader(data)
	mustReadValueFromBuffer(buf, &o.cmdType)
	o.key = mustReadBytesFromBuffer(buf)
	o.value = mustReadBytesFromBuffer(buf)
}

func (o *CmdObject) FromCmd(c Cmd) {
	o.cmdType = byte(c.Type)
	o.key = []byte(c.Key)
	o.value = c.Value
}

func (o *CmdObject) ToCmd() Cmd {
	builder := CmdFactory(int(o.cmdType))
	return builder(string(o.key), o.value)
}

type ResultObject struct {
	code  byte
	value []byte
}

func (o *ResultObject) Header() header {
	return header{
		byte(ResultObjectType),
		uint32(len(o.Body())),
	}
}

func (o *ResultObject) Body() []byte {
	buf := new(bytes.Buffer)
	mustDumpValueToBuffer(buf, o.code)
	mustDumpBytesToBuffer(buf, o.value)
	return buf.Bytes()
}

func (o *ResultObject) Create(data []byte) {
	buf := bytes.NewReader(data)
	mustReadValueFromBuffer(buf, &o.code)
	o.value = mustReadBytesFromBuffer(buf)
}

func (o *ResultObject) FromResult(r *Result) {
	o.code = byte(r.Type())
	if r.Type() == ValueResultCode {
		o.value = r.Value()
	}
	if r.Type() == ErrResultCode {
		o.value = []byte(r.Error())
	}
}

func (o *ResultObject) ToResult() *Result {
	// TODO: handle not-found builder error
	builder := ResultFactory(int(o.code))
	if builder == nil {
		return nil
	}
	return builder(o.value)
}
