package utils

func Memcmp(a []byte, b []byte) int {
	for pos := 0; pos < len(a) && pos < len(b); pos++ {
		if a[pos] > b[pos] {
			return 1
		}
		if a[pos] < b[pos] {
			return -1
		}
	}
	if len(a) > len(b) {
		return 1
	}
	if len(b) > len(a) {
		return -1
	}
	return 0
}
