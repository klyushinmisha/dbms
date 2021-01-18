package pkg

import (
	"errors"
	"io"
)

var ErrReadFailed = errors.New("read failed")

// PageReader implements io.Reader interface
type PageReader struct {
	reader io.Reader
	offset int
	page   []byte
}

func NewPageReader(reader io.Reader, pageSize int64) *PageReader {
	return &PageReader{
		reader: reader,
		offset: 0,
		page:   make([]byte, pageSize, pageSize),
	}
}

func (rd *PageReader) Read(p []byte) (int, error) {
	var pageSize = len(rd.page)
	var totalBytesToRead = len(p)
	var bytesToRead int
	var pageBytesLeft int

	for len(p) > 0 {
		if rd.offset == 0 {
			bytesReadToPage, readErr := rd.reader.Read(rd.page)
			if readErr != nil {
				return 0, readErr
			}
			if bytesReadToPage != len(rd.page) {
				return 0, ErrReadFailed
			}
		}
		pageBytesLeft = pageSize - rd.offset
		if len(p) >= pageBytesLeft {
			bytesToRead = pageBytesLeft
		} else {
			bytesToRead = len(p)
		}
		copy(p[:bytesToRead], rd.page[rd.offset:])
		p = p[bytesToRead:]
		rd.offset = (rd.offset + bytesToRead) % pageSize
	}

	return totalBytesToRead, nil
}
