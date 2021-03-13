package access

import (
	"dbms/pkg/storage"
	"errors"
)

// TODO: set more accurate value
const t = 100

type BPlusTreeNode struct {
	Leaf     bool
	Parent   int64
	Left     int64
	Right    int64
	Size     int32
	Keys     []string
	Pointers []int64
}

func CreateDefaultNode() *BPlusTreeNode {
	var n BPlusTreeNode
	n.Leaf = false
	n.Parent = -1
	n.Left = -1
	n.Right = -1
	n.Size = 0
	n.Keys = make([]string, 2*t, 2*t)
	n.Pointers = make([]int64, 2*t+1, 2*t+1)
	return &n
}

type BPlusTree struct {
	disk *storage.DiskIO
}

func (tree BPlusTree) readNodeFromDisk(pos int64) *BPlusTreeNode {
	var n BPlusTreeNode
	diskNode := tree.disk.ReadIndexPage(pos).ReadIndexNode()
	n.Leaf = diskNode.Leaf
	n.Parent = diskNode.Parent
	n.Left = diskNode.Left
	n.Right = diskNode.Right
	n.Size = diskNode.Size
	n.Keys = diskNode.Keys
	n.Pointers = diskNode.Pointers
	return &n
}

func (tree BPlusTree) writeNodeOnDisk(n *BPlusTreeNode, pos int64) {
	var diskNode storage.BPlusTreeNode
	diskNode.Leaf = n.Leaf
	diskNode.Parent = n.Parent
	diskNode.Left = n.Left
	diskNode.Right = n.Right
	diskNode.Size = n.Size
	diskNode.Keys = n.Keys
	diskNode.Pointers = n.Pointers
	page := storage.AllocateIndexPage()
	page.WriteIndexNode(&diskNode)
	tree.disk.WritePage(pos, page.HeapPage)
}

func MakeBPlusTree(dIo *storage.DiskIO) BPlusTree {
	// cache := MakeLinkedListCache()
	return BPlusTree{dIo}
}

var ErrKeyNotFound = errors.New("provided key not found")

func (tree BPlusTree) findLeafAddr(key string) int64 {
	var pCurNode *BPlusTreeNode
	pHeader := tree.readNodeFromDisk(0)
	var curNodePos = pHeader.Pointers[0]
	pCurNode = tree.readNodeFromDisk(curNodePos)
	// find leaf
	for !pCurNode.Leaf {
		for i := int32(0); i <= pCurNode.Size; i++ {
			if i == pCurNode.Size || key < pCurNode.Keys[i] {
				curNodePos = pCurNode.Pointers[i]
				pCurNode = tree.readNodeFromDisk(curNodePos)
				break
			}
		}
	}
	return curNodePos
}

func (tree BPlusTree) Find(key string) (int64, error) {
	blobKey := tostring(key)
	pLeaf := tree.readNodeFromFile(tree.findLeafAddr(blobKey))
	pos := pLeaf.findKeyPos(blobKey)
	if pos == -1 {
		return 0, ErrKeyNotFound
	}
	return pLeaf.Pointers[pos], nil
}

func (tree BPlusTree) split(pCurNode *BPlusTreeNode, pos int64) {
	pHeader := tree.readHeaderFromFile()
	for {
		midKey := pCurNode.Keys[t]
		midPointer := pCurNode.Pointers[t]
		// generate new BPlusTreeNode address
		nextAddr := tree.getNextBlockAddr()
		rightAddr := pCurNode.Right
		// update current BPlusTreeNode
		pCurNode.Right = nextAddr
		pCurNode.Size = t
		tree.writeNodeToFile(pCurNode, pos)
		// bind it to right neighbour
		if rightAddr != -1 {
			pRightNode := tree.readNodeFromFile(rightAddr)
			pRightNode.Left = nextAddr
			tree.writeNodeToFile(pRightNode, rightAddr)
		}
		// create new BPlusTreeNode
		pNewNode := CreateDefaultNode()
		pNewNode.Parent = pCurNode.Parent
		pNewNode.Left = pos
		pNewNode.Right = rightAddr
		pNewNode.Size = t - 1
		copy(pNewNode.Keys[:pNewNode.Size], pCurNode.Keys[t+1:])
		if pCurNode.Leaf {
			copy(pNewNode.Pointers[:pNewNode.Size], pCurNode.Pointers[t+1:])
		} else {
			copy(pNewNode.Pointers[:pNewNode.Size+1], pCurNode.Pointers[t+1:])
		}
		if pCurNode.Leaf {
			pNewNode.Leaf = true
			pNewNode.putKey(0, midKey, midPointer)
		} else {
			tree.rebindParent(pNewNode, nextAddr)
		}
		tree.writeNodeToFile(pNewNode, nextAddr)
		mustContinue := false
		if pos == pHeader.Head {
			// generate new address for current BPlusTreeNode and bind it to new BPlusTreeNode
			// relies on fact that root BPlusTreeNode has no left neighbour, so rebinding required only for right one == new one
			newRootAddr := tree.getNextBlockAddr()
			pHeader.Head = newRootAddr
			pCurNode.Parent = newRootAddr
			pNewNode.Parent = newRootAddr
			tree.writeHeaderToFile(pHeader)
			tree.writeNodeToFile(pCurNode, pos)
			tree.writeNodeToFile(pNewNode, nextAddr)
			// create new root and write it
			pNewRoot := CreateDefaultNode()
			pNewRoot.Size = 1
			pNewRoot.Keys[0] = midKey
			pNewRoot.Pointers[0] = pos
			pNewRoot.Pointers[1] = nextAddr
			tree.writeNodeToFile(pNewRoot, newRootAddr)
		} else {
			pos = pCurNode.Parent
			pCurNode = tree.readNodeFromFile(pos)
			var p int32 = 0
			for ; p < pCurNode.Size && memcmp(pCurNode.Keys[p], midKey) == -1; p++ {
			}
			// add midKey into BPlusTreeNode
			copy(pCurNode.Keys[p+1:], pCurNode.Keys[p:])
			copy(pCurNode.Pointers[p+2:], pCurNode.Pointers[p+1:])
			pCurNode.Keys[p] = midKey
			pCurNode.Pointers[p+1] = nextAddr
			pCurNode.Size++
			tree.writeNodeToFile(pCurNode, pos)
			// set the flag to run another iteration
			mustContinue = pCurNode.Size == 2*t
		}
		// write previous root to a new location
		if !mustContinue {
			break
		}
	}
}

func (pNode *BPlusTreeNode) putKey(pos int32, key string, pointer int64) {
	copy(pNode.Keys[pos+1:], pNode.Keys[pos:])
	copy(pNode.Pointers[pos+1:], pNode.Pointers[pos:])
	pNode.Keys[pos] = key
	pNode.Pointers[pos] = pointer
	pNode.Size++
}

func (pNode *BPlusTreeNode) popKey(pos int32) {
	copy(pNode.Keys[pos:], pNode.Keys[pos+1:])
	if pNode.Leaf() {
		copy(pNode.Pointers[pos:], pNode.Pointers[pos+1:])
	} else {
		copy(pNode.Pointers[pos+1:], pNode.Pointers[pos+2:])
	}
	pNode.Size--
}

func (tree BPlusTree) Insert(key string, pointer int64) error {
	blobKey := tostring(key)
	BPlusTreeNodeAddr := tree.findLeafAddr(blobKey)
	pLeaf := tree.readNodeFromFile(BPlusTreeNodeAddr)
	// find write position in leaf
	var pos int32 = 0
	for ; pos < pLeaf.Size; pos++ {
		cmpRes := memcmp(blobKey, pLeaf.Keys[pos])
		if cmpRes == 0 {
			// check if key exists; only change addr value
			pLeaf.Pointers[pos] = pointer
			tree.writeNodeToFile(pLeaf, BPlusTreeNodeAddr)
			return nil
		} else if cmpRes == -1 {
			break
		}
	}
	pLeaf.putKey(pos, blobKey, BPlusTreeNodeAddr)
	tree.writeNodeToFile(pLeaf, BPlusTreeNodeAddr)
	// balance tree
	if pLeaf.Size == 2*t {
		tree.split(pLeaf, BPlusTreeNodeAddr)
	}
	return nil
}

func (tree BPlusTree) Init() {
	if tree.disk.IsFileEmpty() {
		hd := CreateDefaultNode()
		tree.writeNodeOnDisk(hd, 0)
		rootPos := tree.disk.GetNextPagePosition()
		hd.Pointers[0] = rootPos
		tree.writeNodeOnDisk(hd, 0)
		pRoot := CreateDefaultNode()
		pRoot.Leaf = true
		tree.writeNodeOnDisk(pRoot, rootPos)
	}
}

func (tree BPlusTree) shiftKeysLeft(pLeft *BPlusTreeNode, pRight *BPlusTreeNode) {
	pLeft.Keys[pLeft.Size] = pRight.Keys[0]
	pLeft.Pointers[pLeft.Size+1] = pRight.Pointers[0]
	copy(pRight.Keys[:], pRight.Keys[1:])
	copy(pRight.Pointers[:], pRight.Pointers[1:])
	pLeft.Size++
	pRight.Size--
}

func (tree BPlusTree) shiftKeysRight(pLeft *BPlusTreeNode, pRight *BPlusTreeNode) {
	copy(pRight.Keys[1:], pRight.Keys[:])
	copy(pRight.Pointers[1:], pRight.Pointers[:])
	pRight.Keys[0] = pLeft.Keys[pLeft.Size-1]
	pRight.Pointers[0] = pLeft.Pointers[pLeft.Size]
	pLeft.Size--
	pRight.Size++
}

func (tree BPlusTree) mergeNodes(pDst *BPlusTreeNode, pSrc *BPlusTreeNode) {
	copy(pDst.Keys[pDst.Size:], pSrc.Keys[:])
	copy(pDst.Pointers[pDst.Size:], pSrc.Pointers[:])
	pDst.Size += pSrc.Size
}

func (tree BPlusTree) mergeInternalNodes(pDst *BPlusTreeNode, pSrc *BPlusTreeNode) {
	pChild := tree.readNodeFromFile(tree.findMinLeaf(pSrc.Pointers[0]))
	pDst.Keys[pDst.Size] = pChild.Keys[0]
	pDst.Pointers[pDst.Size+1] = pSrc.Pointers[0]
	pDst.Size++
	copy(pDst.Keys[pDst.Size:], pSrc.Keys[:])
	copy(pDst.Pointers[pDst.Size+1:], pSrc.Pointers[1:])
	pDst.Size += pSrc.Size
}

func (pNode *BPlusTreeNode) findKeyPos(key string) int32 {
	for pos := int32(0); pos < pNode.Size; pos++ {
		if memcmp(key, pNode.Keys[pos]) == 0 {
			return pos
		}
	}
	return -1
}

func (tree BPlusTree) unlinkNode(pNode *BPlusTreeNode) {
	if pNode.Left != -1 {
		pLeft := tree.readNodeFromFile(pNode.Left)
		pLeft.Right = pNode.Right
		tree.writeNodeToFile(pLeft, pNode.Left)
	}
	if pNode.Right != -1 {
		pRight := tree.readNodeFromFile(pNode.Right)
		pRight.Left = pNode.Left
		tree.writeNodeToFile(pRight, pNode.Right)
	}
}

func (tree BPlusTree) rebindParent(pNode *BPlusTreeNode, newParent int64) {
	for i := int32(0); i <= pNode.Size; i++ {
		pChild := tree.readNodeFromFile(pNode.Pointers[i])
		pChild.Parent = newParent
		tree.writeNodeToFile(pChild, pNode.Pointers[i])
	}
}

func (tree BPlusTree) findMinLeaf(BPlusTreeNodeAddr int64) int64 {
	pNode := tree.readNodeFromFile(BPlusTreeNodeAddr)
	for !pNode.Leaf() {
		BPlusTreeNodeAddr = pNode.Pointers[0]
		pNode = tree.readNodeFromFile(BPlusTreeNodeAddr)
	}
	return BPlusTreeNodeAddr
}

func (tree BPlusTree) updatePathToRoot(BPlusTreeNodeAddr int64) {
	pNode := tree.readNodeFromFile(BPlusTreeNodeAddr)
	minKey := pNode.Keys[0]
	for pNode.Parent != -1 {
		if pNode.Left == -1 {
			return
		}
		pLeftNode := tree.readNodeFromFile(pNode.Left)
		if pLeftNode.Parent == pNode.Parent {
			pParent := tree.readNodeFromFile(pNode.Parent)
			for i := int32(0); i <= pParent.Size; i++ {
				if pParent.Pointers[i] == BPlusTreeNodeAddr {
					pParent.Keys[i-1] = minKey
					tree.writeNodeToFile(pParent, pNode.Parent)
					break
				}
			}
			return
		}
		BPlusTreeNodeAddr = pNode.Parent
		pNode = tree.readNodeFromFile(BPlusTreeNodeAddr)
	}
}

func (tree BPlusTree) deleteInternal(BPlusTreeNodeAddr int64, key string, removeFirst bool) {
	for {
		var pCurNode = tree.readNodeFromFile(BPlusTreeNodeAddr)
		if removeFirst {
			copy(pCurNode.Keys[0:], pCurNode.Keys[1:])
			copy(pCurNode.Pointers[0:], pCurNode.Pointers[1:])
			pCurNode.Size--
			tree.writeNodeToFile(pCurNode, BPlusTreeNodeAddr)
			tree.updatePathToRoot(tree.findMinLeaf(BPlusTreeNodeAddr))
		} else {
			pos := pCurNode.findKeyPos(key)
			if pos == -1 {
				return
			}
			pCurNode.popKey(pos)
			tree.writeNodeToFile(pCurNode, BPlusTreeNodeAddr)
		}
		removeFirst = false
		if pCurNode.Size >= t-1 {
			return
		}
		// balance tree
		var pLeftNode *BPlusTreeNode
		var pRightNode *BPlusTreeNode
		if pCurNode.Left != -1 {
			pLeftNode = tree.readNodeFromFile(pCurNode.Left)
		}
		if pCurNode.Right != -1 {
			pRightNode = tree.readNodeFromFile(pCurNode.Right)
		}
		if pLeftNode != nil && pLeftNode.Size > t-1 {
			tree.shiftKeysRight(pLeftNode, pCurNode)
			pChild := tree.readNodeFromFile(pCurNode.Pointers[0])
			pChild.Parent = BPlusTreeNodeAddr
			tree.writeNodeToFile(pChild, pCurNode.Pointers[0])
			tree.writeNodeToFile(pLeftNode, pCurNode.Left)
			tree.writeNodeToFile(pCurNode, BPlusTreeNodeAddr)
			tree.updatePathToRoot(tree.findMinLeaf(pCurNode.Pointers[0]))
			tree.updatePathToRoot(tree.findMinLeaf(pCurNode.Pointers[1]))
			tree.updatePathToRoot(tree.findMinLeaf(pCurNode.Left))
			return
		} else if pRightNode != nil && pRightNode.Size > t-1 {
			tree.shiftKeysLeft(pCurNode, pRightNode)
			pChild := tree.readNodeFromFile(pCurNode.Pointers[pCurNode.Size])
			pChild.Parent = BPlusTreeNodeAddr
			tree.writeNodeToFile(pChild, pCurNode.Pointers[pCurNode.Size])
			tree.writeNodeToFile(pCurNode, BPlusTreeNodeAddr)
			tree.writeNodeToFile(pRightNode, pCurNode.Right)
			tree.updatePathToRoot(tree.findMinLeaf(pCurNode.Pointers[pCurNode.Size]))
			tree.updatePathToRoot(tree.findMinLeaf(pCurNode.Right))
			return
		} else {
			if pLeftNode != nil {
				tree.rebindParent(pCurNode, pCurNode.Left)
				tree.mergeInternalNodes(pLeftNode, pCurNode)
				pCurNode.SetUsed(false)
				tree.writeNodeToFile(pCurNode, BPlusTreeNodeAddr)
				tree.writeNodeToFile(pLeftNode, pCurNode.Left)
				tree.unlinkNode(pCurNode)
				if pCurNode.Parent == -1 {
					return
				}
				key = tree.readNodeFromFile(tree.findMinLeaf(BPlusTreeNodeAddr)).Keys[0]
				BPlusTreeNodeAddr = pCurNode.Parent
				removeFirst = pLeftNode.Parent != pCurNode.Parent
			} else if pRightNode != nil {
				tree.rebindParent(pRightNode, BPlusTreeNodeAddr)
				tree.mergeInternalNodes(pCurNode, pRightNode)
				pRightNode.SetUsed(false)
				tree.writeNodeToFile(pRightNode, pCurNode.Right)
				tree.writeNodeToFile(pCurNode, BPlusTreeNodeAddr)
				tree.unlinkNode(pRightNode)
				if pCurNode.Parent == -1 {
					return
				}
				BPlusTreeNodeAddr = pCurNode.Parent
				key = tree.readNodeFromFile(tree.findMinLeaf(pCurNode.Right)).Keys[0]
			} else {
				// root deletion case
				pHeader := tree.readHeaderFromFile()
				pRoot := tree.readNodeFromFile(pHeader.Head)
				if pRoot.Size == 0 {
					pRoot.SetUsed(false)
					tree.writeNodeToFile(pRoot, pHeader.Head)
					pHeader.Head = pCurNode.Pointers[0]
					tree.writeHeaderToFile(pHeader)
					pCurNode = tree.readNodeFromFile(pHeader.Head)
					pCurNode.Left = -1
					pCurNode.Right = -1
					pCurNode.Parent = -1
					tree.writeNodeToFile(pCurNode, pHeader.Head)
				}
				return
			}
		}
	}
}

func (tree BPlusTree) Delete(key string) error {
	blobKey := tostring(key)
	BPlusTreeNodeAddr := tree.findLeafAddr(blobKey)
	pLeaf := tree.readNodeFromFile(BPlusTreeNodeAddr)
	pos := pLeaf.findKeyPos(blobKey)
	if pos == -1 {
		return ErrKeyNotFound
	}
	pLeaf.popKey(pos)
	tree.writeNodeToFile(pLeaf, BPlusTreeNodeAddr)
	tree.updatePathToRoot(BPlusTreeNodeAddr)
	if pLeaf.Size >= t-1 {
		return nil
	}
	// balance tree
	var pLeftNode *BPlusTreeNode
	var pRightNode *BPlusTreeNode
	if pLeaf.Left != -1 {
		pLeftNode = tree.readNodeFromFile(pLeaf.Left)
	}
	if pLeaf.Right != -1 {
		pRightNode = tree.readNodeFromFile(pLeaf.Right)
	}
	if pLeftNode != nil && pLeftNode.Size > t-1 {
		tree.shiftKeysRight(pLeftNode, pLeaf)
		tree.writeNodeToFile(pLeftNode, pLeaf.Left)
		tree.writeNodeToFile(pLeaf, BPlusTreeNodeAddr)
		tree.updatePathToRoot(BPlusTreeNodeAddr)
	} else if pRightNode != nil && pRightNode.Size > t-1 {
		tree.shiftKeysLeft(pLeaf, pRightNode)
		tree.writeNodeToFile(pRightNode, pLeaf.Right)
		tree.writeNodeToFile(pLeaf, BPlusTreeNodeAddr)
		tree.updatePathToRoot(BPlusTreeNodeAddr)
		tree.updatePathToRoot(pLeaf.Right)
	} else {
		if pLeftNode != nil {
			tree.mergeNodes(pLeftNode, pLeaf)
			pLeaf.SetUsed(false)
			tree.writeNodeToFile(pLeaf, BPlusTreeNodeAddr)
			tree.writeNodeToFile(pLeftNode, pLeaf.Left)
			tree.unlinkNode(pLeaf)
			tree.updatePathToRoot(BPlusTreeNodeAddr)
			tree.deleteInternal(pLeaf.Parent, pLeaf.Keys[0], pLeftNode.Parent != pLeaf.Parent)
		} else if pRightNode != nil {
			tree.mergeNodes(pLeaf, pRightNode)
			pRightNode.SetUsed(false)
			tree.writeNodeToFile(pRightNode, pLeaf.Right)
			tree.writeNodeToFile(pLeaf, BPlusTreeNodeAddr)
			tree.unlinkNode(pRightNode)
			tree.updatePathToRoot(BPlusTreeNodeAddr)
			tree.deleteInternal(pLeaf.Parent, pRightNode.Keys[0], false)
		}
	}
	return nil
}
