package storage

type BitArray uint8

func (arr *BitArray) Set(value bool, pos int) {
	if value {
		*arr |= 1 << pos
	} else {
		bitMask := BitArray(^(1 << pos))
		*arr &= bitMask
	}
}

func (arr BitArray) Get(pos int) bool {
	return (arr>>pos)&1 == 1
}
