package transfer

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestObject_Cmd(t *testing.T) {
	data := make([]byte, 128, 128)
	cmd := GetCmd("HELLO")
	cmdObj := new(CmdObject)
	cmdObj.FromCmd(cmd)
	output := ObjectWriter{bytes.NewBuffer(data[0:0])}
	output.WriteObject(cmdObj)
	otherCmdObj := new(CmdObject)
	input := ObjectReader{bytes.NewReader(data)}
	input.ReadObject(otherCmdObj)
	assert.Equal(t, otherCmdObj.ToCmd(), cmd)
}

func TestObject_Result(t *testing.T) {
	data := make([]byte, 128, 128)
	res := StrErrResult("Error")
	resObj := new(ResultObject)
	resObj.FromResult(res)
	output := ObjectWriter{bytes.NewBuffer(data[0:0])}
	output.WriteObject(resObj)
	otherResObj := new(ResultObject)
	input := ObjectReader{bytes.NewReader(data)}
	input.ReadObject(otherResObj)
	assert.Equal(t, otherResObj.ToResult(), res)
}
