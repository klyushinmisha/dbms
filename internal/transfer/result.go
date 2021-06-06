package transfer

const (
	OkResultCode    = 0
	ValueResultCode = 1
	ErrResultCode   = 2
)

type Result struct {
	code  int
	value []byte
	err   string
}

func OkResult() *Result {
	r := new(Result)
	r.code = OkResultCode
	return r
}

func ValueResult(value []byte) *Result {
	r := new(Result)
	r.code = ValueResultCode
	r.value = value
	return r
}

func StrErrResult(err string) *Result {
	r := new(Result)
	r.code = ErrResultCode
	r.err = err
	return r
}

func ErrResult(err error) *Result {
	return StrErrResult(err.Error())
}

func (r *Result) Ok() bool {
	return r.code != ErrResultCode
}

func (r *Result) Type() int {
	return r.code
}

func (r *Result) Value() []byte {
	return r.value
}

func (r *Result) Error() string {
	return r.err
}

type resultBuilder func([]byte) *Result

func ResultFactory(code int) resultBuilder {
	switch code {
	case OkResultCode:
		return func(_ []byte) *Result {
			return OkResult()
		}
	case ValueResultCode:
		return func(value []byte) *Result {
			return ValueResult(value)
		}
	case ErrResultCode:
		return func(err []byte) *Result {
			return StrErrResult(string(err))
		}
	}
	return nil
}
