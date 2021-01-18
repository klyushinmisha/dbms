package pkg

// Page is wrapper type over byte slice representing OS page
type Page struct {
	Data []byte
	Size int64
}

func NewPage(pageSize int64) *Page {
	return &Page{
		Data: make([]byte, pageSize, pageSize),
		Size: pageSize,
	}
}
