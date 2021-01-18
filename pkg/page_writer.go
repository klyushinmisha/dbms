package pkg

import (
	"errors"
	"io"
)

var ErrFlushFailed = errors.New("flush failed")

// PageWriter implements io.Writer interface
type PageWriter struct {
	writer io.Writer
	offset int
	page   []byte
}

func NewPageWriter(writer io.Writer, pageSize int64) *PageWriter {
	return &PageWriter{
		writer: writer,
		offset: 0,
		page:   make([]byte, pageSize, pageSize),
	}
}

// TODO: restore readWriteFile on flush fail

func (wr *PageWriter) Flush() error {
	var pageSize = len(wr.page)
	if wr.offset == 0 {
		return nil
	}

	n, err := wr.writer.Write(wr.page)
	if n != pageSize {
		return ErrFlushFailed
	}
	for i := 0; i < pageSize; i++ {
		wr.page[i] = 0
	}
	wr.offset = 0
	return err
}

func (wr *PageWriter) Write(p []byte) (int, error) {
	var pageSize = len(wr.page)
	var totalBytesToWrite = len(p)
	var bytesToWrite int
	var pageBytesLeft int

	for len(p) > 0 {
		pageBytesLeft = pageSize - wr.offset
		if len(p) >= pageBytesLeft {
			bytesToWrite = pageBytesLeft
		} else {
			bytesToWrite = len(p)
		}
		copy(wr.page[wr.offset:], p[:bytesToWrite])
		p = p[bytesToWrite:]
		wr.offset += bytesToWrite
		if wr.offset == pageSize {
			flushErr := wr.Flush()
			if flushErr != nil {
				return 0, flushErr
			}
		}
	}

	return totalBytesToWrite, nil
}
