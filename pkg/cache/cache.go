package cache

type Cache interface {
	Put(int64, interface{}) (int64, interface{})
	Get(int64) (interface{}, bool)
	PruneAll(func(int64, interface{}))
}
