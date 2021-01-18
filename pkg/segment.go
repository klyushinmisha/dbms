package pkg

type Segment struct {
	nextSegmentPos int64
	data           []byte
	pageSize       int64
}
