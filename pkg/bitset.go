package pkg

type Bitset interface {
	Set(addr int64) error
	Reset(addr int64) error
	Check(addr int64) (bool, error)
	Flush() error
}
