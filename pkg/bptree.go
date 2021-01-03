package pkg

const (
	T         = 4
	NodeSize  = 2 * T
	NilNodeId = -1
)

type BPlusTreeNode struct {
	isLeaf   bool
	id       int
	parent   int
	left     int
	right    int
	size     int
	keys     [NodeSize]string
	addrs    [NodeSize]int64
	children [NodeSize + 1]int
}

type BPlusTree struct {
	root  int
	nodes []*BPlusTreeNode
}

func CreateBPlusTree() *BPlusTree {
	return &BPlusTree{
		root: 0,
		nodes: []*BPlusTreeNode{
			&BPlusTreeNode{
				isLeaf: true,
				id:     0,
				parent: NilNodeId,
				left:   NilNodeId,
				right:  NilNodeId,
			},
		},
	}
}

func (tree *BPlusTree) findLeaf(key string) *BPlusTreeNode {
	cur := tree.nodes[tree.root]
	for !cur.isLeaf {
		for i := 0; i <= cur.size; i++ {
			k := cur.keys[i]
			if key < k || cur.size == i {
				cur = tree.nodes[cur.children[i]]
				break
			}
		}
	}
	return cur
}

func (tree *BPlusTree) Find(key string) (int64, error) {
	cur := tree.findLeaf(key)
	for i, k := range cur.keys {
		if key == k {
			return cur.addrs[i], nil
		}
	}
	return 0, ErrKeyNotFound
}

func (node *BPlusTreeNode) splitNode(tree *BPlusTree) {
	curNode := node

	for {
		// create left node and move half of page from node
		leftNode := BPlusTreeNode{
			isLeaf: curNode.isLeaf,
			parent: curNode.parent,
			id:     curNode.id,
			left:   curNode.left,
			size:   T,
		}
		copy(leftNode.keys[:], curNode.keys[0:T])
		copy(leftNode.addrs[:], curNode.addrs[0:T])
		copy(leftNode.children[:], curNode.children[0:T])
		tree.nodes[leftNode.id] = &leftNode

		// create right node and move half of page from node
		rightNode := BPlusTreeNode{
			isLeaf: curNode.isLeaf,
			parent: curNode.parent,
			id:     len(tree.nodes),
			right:  curNode.right,
			size:   T,
		}
		copy(rightNode.keys[:], curNode.keys[T:])
		copy(rightNode.addrs[:], curNode.addrs[T:])
		copy(rightNode.children[:], curNode.children[T:])
		tree.nodes = append(tree.nodes, &rightNode)

		// bind connections
		leftNode.right = rightNode.id
		rightNode.left = leftNode.id

		midKey := rightNode.keys[0]

		// balance tree
		if leftNode.id == tree.root {
			rootNode := BPlusTreeNode{
				isLeaf: false,
				parent: NilNodeId,
				id:     len(tree.nodes),
				left:   NilNodeId,
				right:  NilNodeId,
			}
			tree.nodes = append(tree.nodes, &rootNode)
			tree.root = rootNode.id
			leftNode.parent = rootNode.id
			rightNode.parent = rootNode.id
			rootNode.keys[0] = midKey
			rootNode.children[0] = leftNode.id
			rootNode.children[1] = rightNode.id
			rootNode.size++
		} else {
			// move one key to parent
			// if parent overflows, then split parent
			parent := tree.nodes[leftNode.parent]
			var insertPos int
			for ; insertPos < parent.size; insertPos++ {
				k := parent.keys[insertPos]
				if midKey < k {
					break
				}
			}
			// ERROR: not adding the key
			size := parent.size
			copy(parent.keys[insertPos+1:], parent.keys[insertPos:size-1])
			parent.keys[insertPos] = midKey

			copy(parent.children[insertPos+1:], parent.children[insertPos:size])
			parent.children[insertPos] = rightNode.id
			parent.size++
			if parent.size == 2*T {
				curNode = parent
				continue
			}
		}
		break
	}
}

func (tree *BPlusTree) Add(key string, addr int64) {
	leaf := tree.findLeaf(key)

	var insertPos int
	for ; insertPos < leaf.size; insertPos++ {
		k := leaf.keys[insertPos]
		if key < k {
			break
		}
	}
	size := len(leaf.keys)
	copy(leaf.keys[insertPos+1:], leaf.keys[insertPos:size-1])
	leaf.keys[insertPos] = key

	copy(leaf.addrs[insertPos+1:], leaf.addrs[insertPos:size-1])
	leaf.addrs[insertPos] = addr

	leaf.size++
	if leaf.size == 2*T {
		leaf.splitNode(tree)
	}
}
