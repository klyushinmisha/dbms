package access

type Index interface {
	Init()
	Find(string) (int64, error)
	Insert(string, int64)
	Delete(string) (int64, error)
}
