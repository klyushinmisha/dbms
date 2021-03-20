package bp_tree

type BPTreeNode struct {
	Leaf     bool
	Parent   int64
	Left     int64
	Right    int64
	Size     int
	Keys     []string
	Pointers []int64
}

func createDefaultNode(t int) *BPTreeNode {
	var n BPTreeNode
	n.Leaf = false
	n.Parent = -1
	n.Left = -1
	n.Right = -1
	n.Size = 0
	n.Keys = make([]string, 2*t, 2*t)
	n.Pointers = make([]int64, 2*t+1, 2*t+1)
	return &n
}

func (n *BPTreeNode) findKeyPos(key string) int {
	for pos := 0; pos < n.Size; pos++ {
		if key == n.Keys[pos] {
			return pos
		}
	}
	return -1
}

func (n *BPTreeNode) putKey(pos int, key string, ptr int64) {
	copy(n.Keys[pos+1:], n.Keys[pos:])
	copy(n.Pointers[pos+1:], n.Pointers[pos:])
	n.Keys[pos] = key
	n.Pointers[pos] = ptr
	n.Size++
}

func (n *BPTreeNode) popKey(pos int) {
	copy(n.Keys[pos:], n.Keys[pos+1:])
	if n.Leaf {
		copy(n.Pointers[pos:], n.Pointers[pos+1:])
	} else {
		copy(n.Pointers[pos+1:], n.Pointers[pos+2:])
	}
	n.Size--
}
