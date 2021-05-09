package bp_tree

import (
	"dbms/pkg/storage/adapters/bp_tree"
	"errors"
	"sync"
)

var (
	ErrKeyNotFound = errors.New("Not found")
)

// locking inspired by Lehman and Yao whitepaper (Efficient Locking for Concurrent Operations on B-Trees)
type BPTree struct {
	// deleteLock allows exclusive deletes or concurrent insert/reads;
	// insertLock makes insert exclusive;
	// this locking scheme guarantee:
	//     1) delete locks other deletes, inserts, reads
	//     2) insert locks other inserts
	// so reads are nearly lock-free (except rare delete calls)
	deleteLock sync.RWMutex
	insertLock sync.Mutex
	t          int
	rw         *bpTreeReaderWriter
}

func NewBPTree(t int, ba *bp_tree.BPTreeAdapter) *BPTree {
	var tree BPTree
	tree.t = t
	tree.rw = NewBPTreeReaderWriter(t, ba)
	return &tree
}

func (t *BPTree) Init() {
	if t.rw.Empty() {
		hdr := createDefaultNode(t.t)
		t.rw.AppendNodeToStorage(hdr)
		root := createDefaultNode(t.t)
		root.Leaf = true
		rootPos := t.rw.AppendNodeToStorage(root)
		hdr.Pointers[0] = rootPos
		t.rw.WriteNodeToStorage(hdr, 0)
	}
}

func (t *BPTree) Find(key string) (int64, error) {
	t.deleteLock.RLock()
	defer t.deleteLock.RUnlock()
	pos := t.findLeafPos(key)
	leaf := t.rw.ReadNodeFromStorage(pos)
	keyPos := leaf.findKeyPos(key)
	if keyPos == -1 {
		return 0, ErrKeyNotFound
	}
	return leaf.Pointers[keyPos], nil
}

func (t *BPTree) Insert(key string, ptr int64) {
	t.deleteLock.RLock()
	defer t.deleteLock.RUnlock()
	t.insertLock.Lock()
	defer t.insertLock.Unlock()
	pos := t.findLeafPos(key)
	leaf := t.rw.ReadNodeFromStorage(pos)
	// find write position in leaf
	keyPos := 0
	for ; keyPos < leaf.Size; keyPos++ {
		if key == leaf.Keys[keyPos] {
			// check if key exists; only change addr value
			leaf.Pointers[keyPos] = ptr
			t.rw.WriteNodeToStorage(leaf, pos)
			return
		} else if key < leaf.Keys[keyPos] {
			break
		}
	}
	leaf.putKey(keyPos, key, ptr)
	t.rw.WriteNodeToStorage(leaf, pos)
	// balance t
	if leaf.Size == 2*t.t {
		t.split(leaf, pos)
	}
}

func (t *BPTree) Delete(key string) (int64, error) {
	t.deleteLock.Lock()
	defer t.deleteLock.Unlock()
	pos := t.findLeafPos(key)
	leaf := t.rw.ReadNodeFromStorage(pos)
	keyPos := leaf.findKeyPos(key)
	if keyPos == -1 {
		return -1, ErrKeyNotFound
	}
	delPtr := leaf.Pointers[keyPos]
	leaf.popKey(keyPos)
	t.rw.WriteNodeToStorage(leaf, pos)
	t.updatePathToRoot(pos)
	if leaf.Size >= t.t-1 {
		return delPtr, nil
	}
	var left *BPTreeNode
	var right *BPTreeNode
	if leaf.Left != -1 {
		left = t.rw.ReadNodeFromStorage(leaf.Left)
	}
	if leaf.Right != -1 {
		right = t.rw.ReadNodeFromStorage(leaf.Right)
	}
	if left != nil && left.Size > t.t-1 {
		t.shiftKeysRight(left, leaf)
		t.rw.WriteNodeToStorage(left, leaf.Left)
		t.rw.WriteNodeToStorage(leaf, pos)
		t.updatePathToRoot(pos)
	} else if right != nil && right.Size > t.t-1 {
		t.shiftKeysLeft(leaf, right)
		t.rw.WriteNodeToStorage(right, leaf.Right)
		t.rw.WriteNodeToStorage(leaf, pos)
		t.updatePathToRoot(pos)
		t.updatePathToRoot(leaf.Right)
	} else {
		if left != nil {
			t.mergeNodes(left, leaf)
			t.rw.WriteNodeToStorage(left, leaf.Left)
			t.unlinkNode(leaf)
			t.updatePathToRoot(pos)
			t.rw.ReleaseNodeInStorage(pos)
			t.deleteInternal(leaf.Parent, leaf.Keys[0], left.Parent != leaf.Parent)
		} else if right != nil {
			t.mergeNodes(leaf, right)
			t.rw.WriteNodeToStorage(leaf, pos)
			t.unlinkNode(right)
			t.updatePathToRoot(pos)
			t.rw.ReleaseNodeInStorage(leaf.Right)
			t.deleteInternal(leaf.Parent, right.Keys[0], false)
		}
	}
	return delPtr, nil
}

func (t *BPTree) findLeafPos(key string) int64 {
	var node *BPTreeNode
	hdr := t.rw.ReadNodeFromStorage(0)
	var pos = hdr.Pointers[0]
	node = t.rw.ReadNodeFromStorage(pos)
	rightPos := node.Right
	for !node.Leaf {
		for i := 0; i <= node.Size; i++ {
			if i == node.Size || key < node.Keys[i] {
				pos = node.Pointers[i]
				node = t.rw.ReadNodeFromStorage(pos)
				break
			}
		}
		// if key is not present in current node (as expected before split iteration),
		// then check right block if exists (value must be there);
		if rightPos != -1 {
			node = t.rw.ReadNodeFromStorage(rightPos)
			for i := 0; i <= node.Size; i++ {
				if i == node.Size || key < node.Keys[i] {
					pos = node.Pointers[i]
					node = t.rw.ReadNodeFromStorage(pos)
					break
				}
			}
		}
	}
	return pos
}

func (t *BPTree) split(node *BPTreeNode, pos int64) {
	hdr := t.rw.ReadNodeFromStorage(0)
	for {
		midKey := node.Keys[t.t]
		midPtr := node.Pointers[t.t]
		rightPos := node.Right
		// generate new BPTreeNode address
		newNode := createDefaultNode(t.t)
		newNode.Parent = node.Parent
		newNode.Left = pos
		newNode.Right = rightPos
		newNode.Size = t.t - 1
		copy(newNode.Keys[:newNode.Size], node.Keys[t.t+1:])
		if node.Leaf {
			copy(newNode.Pointers[:newNode.Size], node.Pointers[t.t+1:])
		} else {
			copy(newNode.Pointers[:newNode.Size+1], node.Pointers[t.t+1:])
		}
		if node.Leaf {
			newNode.Leaf = true
			newNode.putKey(0, midKey, midPtr)
		}
		newPos := t.rw.AppendNodeToStorage(newNode)
		if !node.Leaf {
			t.rebindParent(newNode, newPos)
		}
		// update current BPTreeNode
		node.Right = newPos
		node.Size = t.t
		t.rw.WriteNodeToStorage(node, pos)
		// bind it to right neighbour
		if rightPos != -1 {
			rightNode := t.rw.ReadNodeFromStorage(rightPos)
			rightNode.Left = newPos
			t.rw.WriteNodeToStorage(rightNode, rightPos)
		}
		mustContinue := false
		if pos == hdr.Pointers[0] {
			// generate new address for current BPTreeNode and bind it to new BPTreeNode
			// relies on fact that root BPTreeNode has no left neighbour, so rebinding required only for right one == new one
			newRoot := createDefaultNode(t.t)
			newRoot.Size = 1
			newRoot.Keys[0] = midKey
			newRoot.Pointers[0] = pos
			newRoot.Pointers[1] = newPos
			newRootPos := t.rw.AppendNodeToStorage(newRoot)
			hdr.Pointers[0] = newRootPos
			node.Parent = newRootPos
			newNode.Parent = newRootPos
			t.rw.WriteNodeToStorage(hdr, 0)
			t.rw.WriteNodeToStorage(node, pos)
			t.rw.WriteNodeToStorage(newNode, newPos)
		} else {
			pos = node.Parent
			node = t.rw.ReadNodeFromStorage(pos)
			p := 0
			for ; p < node.Size && node.Keys[p] < midKey; p++ {
			}
			// add midKey into BPTreeNode
			copy(node.Keys[p+1:], node.Keys[p:])
			copy(node.Pointers[p+2:], node.Pointers[p+1:])
			node.Keys[p] = midKey
			node.Pointers[p+1] = newPos
			node.Size++
			t.rw.WriteNodeToStorage(node, pos)
			// set the flag to run another iteration
			mustContinue = node.Size == 2*t.t
		}
		// write previous root to a new location
		if !mustContinue {
			break
		}
	}
}

func (t *BPTree) shiftKeysLeft(left *BPTreeNode, right *BPTreeNode) {
	left.Keys[left.Size] = right.Keys[0]
	left.Pointers[left.Size] = right.Pointers[0]
	copy(right.Keys[:], right.Keys[1:])
	copy(right.Pointers[:], right.Pointers[1:])
	left.Size++
	right.Size--
}

func (t *BPTree) shiftKeysLeftInternal(left *BPTreeNode, right *BPTreeNode) {
	left.Keys[left.Size] = right.Keys[0]
	left.Pointers[left.Size+1] = right.Pointers[1]
	copy(right.Keys[:], right.Keys[1:])
	copy(right.Pointers[:], right.Pointers[1:])
	left.Size++
	right.Size--
}

func (t *BPTree) shiftKeysRight(left *BPTreeNode, right *BPTreeNode) {
	copy(right.Keys[1:], right.Keys[:])
	copy(right.Pointers[1:], right.Pointers[:])
	right.Keys[0] = left.Keys[left.Size-1]
	right.Pointers[0] = left.Pointers[left.Size-1]
	left.Size--
	right.Size++
}

func (t *BPTree) shiftKeysRightInternal(left *BPTreeNode, right *BPTreeNode) {
	copy(right.Keys[1:], right.Keys[:])
	copy(right.Pointers[1:], right.Pointers[:])
	right.Keys[0] = left.Keys[left.Size-1]
	right.Pointers[0] = left.Pointers[left.Size-2]
	left.Size--
	right.Size++
}

func (t *BPTree) mergeNodes(dst *BPTreeNode, src *BPTreeNode) {
	copy(dst.Keys[dst.Size:], src.Keys[:])
	copy(dst.Pointers[dst.Size:], src.Pointers[:])
	dst.Size += src.Size
}

func (t *BPTree) mergeInternalNodes(dst *BPTreeNode, src *BPTreeNode) {
	chld := t.rw.ReadNodeFromStorage(t.findMinLeaf(src.Pointers[0]))
	dst.Keys[dst.Size] = chld.Keys[0]
	dst.Pointers[dst.Size+1] = src.Pointers[0]
	dst.Size++
	copy(dst.Keys[dst.Size:], src.Keys[:])
	copy(dst.Pointers[dst.Size+1:], src.Pointers[1:])
	dst.Size += src.Size
}

func (t *BPTree) unlinkNode(n *BPTreeNode) {
	if n.Left != -1 {
		left := t.rw.ReadNodeFromStorage(n.Left)
		left.Right = n.Right
		t.rw.WriteNodeToStorage(left, n.Left)
	}
	if n.Right != -1 {
		right := t.rw.ReadNodeFromStorage(n.Right)
		right.Left = n.Left
		t.rw.WriteNodeToStorage(right, n.Right)
	}
}

func (t *BPTree) rebindParent(n *BPTreeNode, parPtr int64) {
	for i := 0; i <= n.Size; i++ {
		chld := t.rw.ReadNodeFromStorage(n.Pointers[i])
		chld.Parent = parPtr
		t.rw.WriteNodeToStorage(chld, n.Pointers[i])
	}
}

func (t *BPTree) findMinLeaf(pos int64) int64 {
	n := t.rw.ReadNodeFromStorage(pos)
	for !n.Leaf {
		pos = n.Pointers[0]
		n = t.rw.ReadNodeFromStorage(pos)
	}
	return pos
}

func (t *BPTree) updatePathToRoot(pos int64) {
	n := t.rw.ReadNodeFromStorage(pos)
	minKey := n.Keys[0]
	for n.Parent != -1 {
		if n.Left == -1 {
			return
		}
		left := t.rw.ReadNodeFromStorage(n.Left)
		if left.Parent == n.Parent {
			par := t.rw.ReadNodeFromStorage(n.Parent)
			for i := 0; i <= par.Size; i++ {
				if par.Pointers[i] == pos {
					par.Keys[i-1] = minKey
					t.rw.WriteNodeToStorage(par, n.Parent)
					break
				}
			}
			return
		}
		pos = n.Parent
		n = t.rw.ReadNodeFromStorage(pos)
	}
}

func (t *BPTree) deleteInternal(pos int64, key string, removeFirst bool) {
	for {
		var node = t.rw.ReadNodeFromStorage(pos)
		if removeFirst {
			copy(node.Keys[0:], node.Keys[1:])
			copy(node.Pointers[0:], node.Pointers[1:])
			node.Size--
			t.rw.WriteNodeToStorage(node, pos)
			t.updatePathToRoot(t.findMinLeaf(pos))
		} else {
			keyPos := node.findKeyPos(key)
			if keyPos == -1 {
				return
			}
			node.popKey(keyPos)
			t.rw.WriteNodeToStorage(node, pos)
		}
		removeFirst = false
		if node.Size >= t.t-1 {
			return
		}
		var left *BPTreeNode
		var right *BPTreeNode
		if node.Left != -1 {
			left = t.rw.ReadNodeFromStorage(node.Left)
		}
		if node.Right != -1 {
			right = t.rw.ReadNodeFromStorage(node.Right)
		}
		if left != nil && left.Size > t.t-1 {
			t.shiftKeysRightInternal(left, node)
			chld := t.rw.ReadNodeFromStorage(node.Pointers[0])
			chld.Parent = pos
			t.rw.WriteNodeToStorage(chld, node.Pointers[0])
			t.rw.WriteNodeToStorage(left, node.Left)
			t.rw.WriteNodeToStorage(node, pos)
			t.updatePathToRoot(t.findMinLeaf(node.Pointers[0]))
			t.updatePathToRoot(t.findMinLeaf(node.Pointers[1]))
			t.updatePathToRoot(t.findMinLeaf(node.Left))
			return
		} else if right != nil && right.Size > t.t-1 {
			t.shiftKeysLeftInternal(node, right)
			chld := t.rw.ReadNodeFromStorage(node.Pointers[node.Size])
			chld.Parent = pos
			t.rw.WriteNodeToStorage(chld, node.Pointers[node.Size])
			t.rw.WriteNodeToStorage(node, pos)
			t.rw.WriteNodeToStorage(right, node.Right)
			t.updatePathToRoot(t.findMinLeaf(node.Pointers[node.Size]))
			t.updatePathToRoot(t.findMinLeaf(node.Right))
			return
		} else {
			if left != nil {
				t.rebindParent(node, node.Left)
				t.mergeInternalNodes(left, node)
				t.rw.WriteNodeToStorage(left, node.Left)
				t.unlinkNode(node)
				t.rw.ReleaseNodeInStorage(pos)
				if node.Parent == -1 {
					return
				}
				key = t.rw.ReadNodeFromStorage(t.findMinLeaf(pos)).Keys[0]
				pos = node.Parent
				removeFirst = left.Parent != node.Parent
			} else if right != nil {
				t.rebindParent(right, pos)
				t.mergeInternalNodes(node, right)
				t.rw.WriteNodeToStorage(node, pos)
				t.unlinkNode(right)
				t.rw.ReleaseNodeInStorage(node.Right)
				if node.Parent == -1 {
					return
				}
				pos = node.Parent
				key = t.rw.ReadNodeFromStorage(t.findMinLeaf(node.Right)).Keys[0]
			} else {
				// root deletion case
				hdr := t.rw.ReadNodeFromStorage(0)
				rootPos := hdr.Pointers[0]
				root := t.rw.ReadNodeFromStorage(rootPos)
				if root.Size == 0 {
					t.rw.ReleaseNodeInStorage(rootPos)
					hdr.Pointers[0] = node.Pointers[0]
					t.rw.WriteNodeToStorage(hdr, 0)
					node = t.rw.ReadNodeFromStorage(hdr.Pointers[0])
					node.Left = -1
					node.Right = -1
					node.Parent = -1
					t.rw.WriteNodeToStorage(node, hdr.Pointers[0])
				}
				return
			}
		}
	}
}
