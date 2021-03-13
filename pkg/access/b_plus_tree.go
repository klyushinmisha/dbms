package access

import (
	"dbms/pkg/storage"
	"errors"
)

type BPlusTreeNode struct {
	Leaf     bool
	Parent   int64
	Left     int64
	Right    int64
	Size     int
	Keys     []string
	Pointers []int64
}

func (tree *BPlusTree) createDefaultNode() *BPlusTreeNode {
	var n BPlusTreeNode
	n.Leaf = false
	n.Parent = -1
	n.Left = -1
	n.Right = -1
	n.Size = 0
	n.Keys = make([]string, 2*tree.t, 2*tree.t)
	n.Pointers = make([]int64, 2*tree.t+1, 2*tree.t+1)
	return &n
}

type BPlusTree struct {
	t  int
	rw *indexReaderWriter
}

func MakeBPlusTree(t int, disk *storage.IndexDiskIO) *BPlusTree {
	// cache := MakeLinkedListCache()
	rw := NewIndexReaderWriter(t, disk)
	return &BPlusTree{t, rw}
}

var ErrKeyNotFound = errors.New("provided key not found")

func (tree *BPlusTree) findLeafAddr(key string) int64 {
	var pCurNode *BPlusTreeNode
	pHeader := tree.rw.ReadNodeFromDisk(0)
	var curNodePos = pHeader.Pointers[0]
	pCurNode = tree.rw.ReadNodeFromDisk(curNodePos)
	// find leaf
	for !pCurNode.Leaf {
		for i := 0; i <= pCurNode.Size; i++ {
			if i == pCurNode.Size || key < pCurNode.Keys[i] {
				curNodePos = pCurNode.Pointers[i]
				pCurNode = tree.rw.ReadNodeFromDisk(curNodePos)
				break
			}
		}
	}
	return curNodePos
}

func (tree *BPlusTree) Find(key string) (int64, error) {
	pLeaf := tree.rw.ReadNodeFromDisk(tree.findLeafAddr(key))
	pos := pLeaf.findKeyPos(key)
	if pos == -1 {
		return 0, ErrKeyNotFound
	}
	return pLeaf.Pointers[pos], nil
}

func (tree *BPlusTree) split(pCurNode *BPlusTreeNode, pos int64) {
	pHeader := tree.rw.ReadNodeFromDisk(0)
	for {
		midKey := pCurNode.Keys[tree.t]
		midPointer := pCurNode.Pointers[tree.t]
		// generate new BPlusTreeNode address
		nextAddr := tree.rw.GetNextPagePosition()
		rightAddr := pCurNode.Right
		// update current BPlusTreeNode
		pCurNode.Right = nextAddr
		pCurNode.Size = tree.t
		tree.rw.WriteNodeOnDisk(pCurNode, pos)
		// bind it to right neighbour
		if rightAddr != -1 {
			pRightNode := tree.rw.ReadNodeFromDisk(rightAddr)
			pRightNode.Left = nextAddr
			tree.rw.WriteNodeOnDisk(pRightNode, rightAddr)
		}
		// create new BPlusTreeNode
		pNewNode := tree.createDefaultNode()
		pNewNode.Parent = pCurNode.Parent
		pNewNode.Left = pos
		pNewNode.Right = rightAddr
		pNewNode.Size = tree.t - 1
		copy(pNewNode.Keys[:pNewNode.Size], pCurNode.Keys[tree.t+1:])
		if pCurNode.Leaf {
			copy(pNewNode.Pointers[:pNewNode.Size], pCurNode.Pointers[tree.t+1:])
		} else {
			copy(pNewNode.Pointers[:pNewNode.Size+1], pCurNode.Pointers[tree.t+1:])
		}
		if pCurNode.Leaf {
			pNewNode.Leaf = true
			pNewNode.putKey(0, midKey, midPointer)
		} else {
			tree.rebindParent(pNewNode, nextAddr)
		}
		tree.rw.WriteNodeOnDisk(pNewNode, nextAddr)
		mustContinue := false
		if pos == pHeader.Pointers[0] {
			// generate new address for current BPlusTreeNode and bind it to new BPlusTreeNode
			// relies on fact that root BPlusTreeNode has no left neighbour, so rebinding required only for right one == new one
			newRootAddr := tree.rw.GetNextPagePosition()
			pHeader.Pointers[0] = newRootAddr
			pCurNode.Parent = newRootAddr
			pNewNode.Parent = newRootAddr
			tree.rw.WriteNodeOnDisk(pHeader, 0)
			tree.rw.WriteNodeOnDisk(pCurNode, pos)
			tree.rw.WriteNodeOnDisk(pNewNode, nextAddr)
			// create new root and write it
			pNewRoot := tree.createDefaultNode()
			pNewRoot.Size = 1
			pNewRoot.Keys[0] = midKey
			pNewRoot.Pointers[0] = pos
			pNewRoot.Pointers[1] = nextAddr
			tree.rw.WriteNodeOnDisk(pNewRoot, newRootAddr)
		} else {
			pos = pCurNode.Parent
			pCurNode = tree.rw.ReadNodeFromDisk(pos)
			p := 0
			for ; p < pCurNode.Size && pCurNode.Keys[p] < midKey; p++ {
			}
			// add midKey into BPlusTreeNode
			copy(pCurNode.Keys[p+1:], pCurNode.Keys[p:])
			copy(pCurNode.Pointers[p+2:], pCurNode.Pointers[p+1:])
			pCurNode.Keys[p] = midKey
			pCurNode.Pointers[p+1] = nextAddr
			pCurNode.Size++
			tree.rw.WriteNodeOnDisk(pCurNode, pos)
			// set the flag to run another iteration
			mustContinue = pCurNode.Size == 2*tree.t
		}
		// write previous root to a new location
		if !mustContinue {
			break
		}
	}
}

func (pNode *BPlusTreeNode) putKey(pos int, key string, pointer int64) {
	copy(pNode.Keys[pos+1:], pNode.Keys[pos:])
	copy(pNode.Pointers[pos+1:], pNode.Pointers[pos:])
	pNode.Keys[pos] = key
	pNode.Pointers[pos] = pointer
	pNode.Size++
}

func (pNode *BPlusTreeNode) popKey(pos int) {
	copy(pNode.Keys[pos:], pNode.Keys[pos+1:])
	if pNode.Leaf {
		copy(pNode.Pointers[pos:], pNode.Pointers[pos+1:])
	} else {
		copy(pNode.Pointers[pos+1:], pNode.Pointers[pos+2:])
	}
	pNode.Size--
}

func (tree *BPlusTree) Insert(key string, pointer int64) {
	nodePos := tree.findLeafAddr(key)
	pLeaf := tree.rw.ReadNodeFromDisk(nodePos)
	// find write position in leaf
	pos := 0
	for ; pos < pLeaf.Size; pos++ {
		if key == pLeaf.Keys[pos] {
			// check if key exists; only change addr value
			pLeaf.Pointers[pos] = pointer
			tree.rw.WriteNodeOnDisk(pLeaf, nodePos)
			return
		} else if key < pLeaf.Keys[pos] {
			break
		}
	}
	pLeaf.putKey(pos, key, pointer)
	tree.rw.WriteNodeOnDisk(pLeaf, nodePos)
	// balance tree
	if pLeaf.Size == 2*tree.t {
		tree.split(pLeaf, nodePos)
	}
}

func (tree *BPlusTree) Init() {
	if tree.rw.Empty() {
		hd := tree.createDefaultNode()
		tree.rw.WriteNodeOnDisk(hd, 0)
		rootPos := tree.rw.GetNextPagePosition()
		hd.Pointers[0] = rootPos
		tree.rw.WriteNodeOnDisk(hd, 0)
		pRoot := tree.createDefaultNode()
		pRoot.Leaf = true
		tree.rw.WriteNodeOnDisk(pRoot, rootPos)
	}
}

func (tree *BPlusTree) shiftKeysLeft(pLeft *BPlusTreeNode, pRight *BPlusTreeNode) {
	pLeft.Keys[pLeft.Size] = pRight.Keys[0]
	pLeft.Pointers[pLeft.Size+1] = pRight.Pointers[0]
	copy(pRight.Keys[:], pRight.Keys[1:])
	copy(pRight.Pointers[:], pRight.Pointers[1:])
	pLeft.Size++
	pRight.Size--
}

func (tree *BPlusTree) shiftKeysRight(pLeft *BPlusTreeNode, pRight *BPlusTreeNode) {
	copy(pRight.Keys[1:], pRight.Keys[:])
	copy(pRight.Pointers[1:], pRight.Pointers[:])
	pRight.Keys[0] = pLeft.Keys[pLeft.Size-1]
	pRight.Pointers[0] = pLeft.Pointers[pLeft.Size]
	pLeft.Size--
	pRight.Size++
}

func (tree *BPlusTree) mergeNodes(pDst *BPlusTreeNode, pSrc *BPlusTreeNode) {
	copy(pDst.Keys[pDst.Size:], pSrc.Keys[:])
	copy(pDst.Pointers[pDst.Size:], pSrc.Pointers[:])
	pDst.Size += pSrc.Size
}

func (tree *BPlusTree) mergeInternalNodes(pDst *BPlusTreeNode, pSrc *BPlusTreeNode) {
	pChild := tree.rw.ReadNodeFromDisk(tree.findMinLeaf(pSrc.Pointers[0]))
	pDst.Keys[pDst.Size] = pChild.Keys[0]
	pDst.Pointers[pDst.Size+1] = pSrc.Pointers[0]
	pDst.Size++
	copy(pDst.Keys[pDst.Size:], pSrc.Keys[:])
	copy(pDst.Pointers[pDst.Size+1:], pSrc.Pointers[1:])
	pDst.Size += pSrc.Size
}

func (pNode *BPlusTreeNode) findKeyPos(key string) int {
	for pos := 0; pos < pNode.Size; pos++ {
		if key == pNode.Keys[pos] {
			return pos
		}
	}
	return -1
}

func (tree *BPlusTree) unlinkNode(pNode *BPlusTreeNode) {
	if pNode.Left != -1 {
		pLeft := tree.rw.ReadNodeFromDisk(pNode.Left)
		pLeft.Right = pNode.Right
		tree.rw.WriteNodeOnDisk(pLeft, pNode.Left)
	}
	if pNode.Right != -1 {
		pRight := tree.rw.ReadNodeFromDisk(pNode.Right)
		pRight.Left = pNode.Left
		tree.rw.WriteNodeOnDisk(pRight, pNode.Right)
	}
}

func (tree *BPlusTree) rebindParent(pNode *BPlusTreeNode, newParent int64) {
	for i := 0; i <= pNode.Size; i++ {
		pChild := tree.rw.ReadNodeFromDisk(pNode.Pointers[i])
		pChild.Parent = newParent
		tree.rw.WriteNodeOnDisk(pChild, pNode.Pointers[i])
	}
}

func (tree *BPlusTree) findMinLeaf(BPlusTreeNodeAddr int64) int64 {
	pNode := tree.rw.ReadNodeFromDisk(BPlusTreeNodeAddr)
	for !pNode.Leaf {
		BPlusTreeNodeAddr = pNode.Pointers[0]
		pNode = tree.rw.ReadNodeFromDisk(BPlusTreeNodeAddr)
	}
	return BPlusTreeNodeAddr
}

func (tree *BPlusTree) updatePathToRoot(BPlusTreeNodeAddr int64) {
	pNode := tree.rw.ReadNodeFromDisk(BPlusTreeNodeAddr)
	minKey := pNode.Keys[0]
	for pNode.Parent != -1 {
		if pNode.Left == -1 {
			return
		}
		pLeftNode := tree.rw.ReadNodeFromDisk(pNode.Left)
		if pLeftNode.Parent == pNode.Parent {
			pParent := tree.rw.ReadNodeFromDisk(pNode.Parent)
			for i := 0; i <= pParent.Size; i++ {
				if pParent.Pointers[i] == BPlusTreeNodeAddr {
					pParent.Keys[i-1] = minKey
					tree.rw.WriteNodeOnDisk(pParent, pNode.Parent)
					break
				}
			}
			return
		}
		BPlusTreeNodeAddr = pNode.Parent
		pNode = tree.rw.ReadNodeFromDisk(BPlusTreeNodeAddr)
	}
}

func (tree *BPlusTree) deleteInternal(BPlusTreeNodeAddr int64, key string, removeFirst bool) {
	for {
		var pCurNode = tree.rw.ReadNodeFromDisk(BPlusTreeNodeAddr)
		if removeFirst {
			copy(pCurNode.Keys[0:], pCurNode.Keys[1:])
			copy(pCurNode.Pointers[0:], pCurNode.Pointers[1:])
			pCurNode.Size--
			tree.rw.WriteNodeOnDisk(pCurNode, BPlusTreeNodeAddr)
			tree.updatePathToRoot(tree.findMinLeaf(BPlusTreeNodeAddr))
		} else {
			pos := pCurNode.findKeyPos(key)
			if pos == -1 {
				return
			}
			pCurNode.popKey(pos)
			tree.rw.WriteNodeOnDisk(pCurNode, BPlusTreeNodeAddr)
		}
		removeFirst = false
		if pCurNode.Size >= tree.t-1 {
			return
		}
		// balance tree
		var pLeftNode *BPlusTreeNode
		var pRightNode *BPlusTreeNode
		if pCurNode.Left != -1 {
			pLeftNode = tree.rw.ReadNodeFromDisk(pCurNode.Left)
		}
		if pCurNode.Right != -1 {
			pRightNode = tree.rw.ReadNodeFromDisk(pCurNode.Right)
		}
		if pLeftNode != nil && pLeftNode.Size > tree.t-1 {
			tree.shiftKeysRight(pLeftNode, pCurNode)
			pChild := tree.rw.ReadNodeFromDisk(pCurNode.Pointers[0])
			pChild.Parent = BPlusTreeNodeAddr
			tree.rw.WriteNodeOnDisk(pChild, pCurNode.Pointers[0])
			tree.rw.WriteNodeOnDisk(pLeftNode, pCurNode.Left)
			tree.rw.WriteNodeOnDisk(pCurNode, BPlusTreeNodeAddr)
			tree.updatePathToRoot(tree.findMinLeaf(pCurNode.Pointers[0]))
			tree.updatePathToRoot(tree.findMinLeaf(pCurNode.Pointers[1]))
			tree.updatePathToRoot(tree.findMinLeaf(pCurNode.Left))
			return
		} else if pRightNode != nil && pRightNode.Size > tree.t-1 {
			tree.shiftKeysLeft(pCurNode, pRightNode)
			pChild := tree.rw.ReadNodeFromDisk(pCurNode.Pointers[pCurNode.Size])
			pChild.Parent = BPlusTreeNodeAddr
			tree.rw.WriteNodeOnDisk(pChild, pCurNode.Pointers[pCurNode.Size])
			tree.rw.WriteNodeOnDisk(pCurNode, BPlusTreeNodeAddr)
			tree.rw.WriteNodeOnDisk(pRightNode, pCurNode.Right)
			tree.updatePathToRoot(tree.findMinLeaf(pCurNode.Pointers[pCurNode.Size]))
			tree.updatePathToRoot(tree.findMinLeaf(pCurNode.Right))
			return
		} else {
			if pLeftNode != nil {
				tree.rebindParent(pCurNode, pCurNode.Left)
				tree.mergeInternalNodes(pLeftNode, pCurNode)
				//pCurNode.SetUsed(false)
				tree.rw.WriteNodeOnDisk(pCurNode, BPlusTreeNodeAddr)
				tree.rw.WriteNodeOnDisk(pLeftNode, pCurNode.Left)
				tree.unlinkNode(pCurNode)
				if pCurNode.Parent == -1 {
					return
				}
				key = tree.rw.ReadNodeFromDisk(tree.findMinLeaf(BPlusTreeNodeAddr)).Keys[0]
				BPlusTreeNodeAddr = pCurNode.Parent
				removeFirst = pLeftNode.Parent != pCurNode.Parent
			} else if pRightNode != nil {
				tree.rebindParent(pRightNode, BPlusTreeNodeAddr)
				tree.mergeInternalNodes(pCurNode, pRightNode)
				//pRightNode.SetUsed(false)
				tree.rw.WriteNodeOnDisk(pRightNode, pCurNode.Right)
				tree.rw.WriteNodeOnDisk(pCurNode, BPlusTreeNodeAddr)
				tree.unlinkNode(pRightNode)
				if pCurNode.Parent == -1 {
					return
				}
				BPlusTreeNodeAddr = pCurNode.Parent
				key = tree.rw.ReadNodeFromDisk(tree.findMinLeaf(pCurNode.Right)).Keys[0]
			} else {
				// root deletion case
				pHeader := tree.rw.ReadNodeFromDisk(0)
				pRoot := tree.rw.ReadNodeFromDisk(pHeader.Pointers[0])
				if pRoot.Size == 0 {
					// pRoot.SetUsed(false)
					tree.rw.WriteNodeOnDisk(pRoot, pHeader.Pointers[0])
					pHeader.Pointers[0] = pCurNode.Pointers[0]
					tree.rw.WriteNodeOnDisk(pHeader, 0)
					pCurNode = tree.rw.ReadNodeFromDisk(pHeader.Pointers[0])
					pCurNode.Left = -1
					pCurNode.Right = -1
					pCurNode.Parent = -1
					tree.rw.WriteNodeOnDisk(pCurNode, pHeader.Pointers[0])
				}
				return
			}
		}
	}
}

func (tree *BPlusTree) Delete(key string) error {
	BPlusTreeNodeAddr := tree.findLeafAddr(key)
	pLeaf := tree.rw.ReadNodeFromDisk(BPlusTreeNodeAddr)
	pos := pLeaf.findKeyPos(key)
	if pos == -1 {
		return ErrKeyNotFound
	}
	pLeaf.popKey(pos)
	tree.rw.WriteNodeOnDisk(pLeaf, BPlusTreeNodeAddr)
	tree.updatePathToRoot(BPlusTreeNodeAddr)
	if pLeaf.Size >= tree.t-1 {
		return nil
	}
	// balance tree
	var pLeftNode *BPlusTreeNode
	var pRightNode *BPlusTreeNode
	if pLeaf.Left != -1 {
		pLeftNode = tree.rw.ReadNodeFromDisk(pLeaf.Left)
	}
	if pLeaf.Right != -1 {
		pRightNode = tree.rw.ReadNodeFromDisk(pLeaf.Right)
	}
	if pLeftNode != nil && pLeftNode.Size > tree.t-1 {
		tree.shiftKeysRight(pLeftNode, pLeaf)
		tree.rw.WriteNodeOnDisk(pLeftNode, pLeaf.Left)
		tree.rw.WriteNodeOnDisk(pLeaf, BPlusTreeNodeAddr)
		tree.updatePathToRoot(BPlusTreeNodeAddr)
	} else if pRightNode != nil && pRightNode.Size > tree.t-1 {
		tree.shiftKeysLeft(pLeaf, pRightNode)
		tree.rw.WriteNodeOnDisk(pRightNode, pLeaf.Right)
		tree.rw.WriteNodeOnDisk(pLeaf, BPlusTreeNodeAddr)
		tree.updatePathToRoot(BPlusTreeNodeAddr)
		tree.updatePathToRoot(pLeaf.Right)
	} else {
		if pLeftNode != nil {
			tree.mergeNodes(pLeftNode, pLeaf)
			// pLeaf.SetUsed(false)
			tree.rw.WriteNodeOnDisk(pLeaf, BPlusTreeNodeAddr)
			tree.rw.WriteNodeOnDisk(pLeftNode, pLeaf.Left)
			tree.unlinkNode(pLeaf)
			tree.updatePathToRoot(BPlusTreeNodeAddr)
			tree.deleteInternal(pLeaf.Parent, pLeaf.Keys[0], pLeftNode.Parent != pLeaf.Parent)
		} else if pRightNode != nil {
			tree.mergeNodes(pLeaf, pRightNode)
			// pRightNode.SetUsed(false)
			tree.rw.WriteNodeOnDisk(pRightNode, pLeaf.Right)
			tree.rw.WriteNodeOnDisk(pLeaf, BPlusTreeNodeAddr)
			tree.unlinkNode(pRightNode)
			tree.updatePathToRoot(BPlusTreeNodeAddr)
			tree.deleteInternal(pLeaf.Parent, pRightNode.Keys[0], false)
		}
	}
	return nil
}
