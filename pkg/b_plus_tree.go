package pkg

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
)

type KeyType [256]byte

func toKeyType(v string) KeyType {
	var ret KeyType
	copy(ret[:], []byte(v)[:])
	return ret
}

func memcmp(a KeyType, b KeyType) int {
	for pos := 0; pos < len(a); pos++ {
		if a[pos] > b[pos] {
			return 1
		}
		if a[pos] < b[pos] {
			return -1
		}
	}
	return 0
}

type AddrType int64

// TODO: set more accurate value
const t = 2

type header struct {
	Head AddrType
}

type node struct {
	Leaf     bool
	Parent   AddrType
	Left     AddrType
	Right    AddrType
	KeyNum   int32
	Keys     [2 * t]KeyType
	Pointers [2 * t]AddrType
	Children [2*t + 1]AddrType
}

func (tree BPlusTree) readHeaderFromFile() *header {
	if tree.isFileEmpty() {
		return nil
	}
	_, seekErr := tree.file.Seek(0, io.SeekStart)
	if seekErr != nil {
		log.Panicln(seekErr)
	}
	var h header
	err := binary.Read(tree.file, binary.LittleEndian, &h)
	if err != nil {
		log.Panicln(err)
	}
	return &h
}

func (tree BPlusTree) writeHeaderToFile(pHeader *header) {
	_, seekErr := tree.file.Seek(0, io.SeekStart)
	if seekErr != nil {
		log.Panicln(seekErr)
	}
	err := binary.Write(tree.file, binary.LittleEndian, *pHeader)
	if err != nil {
		log.Panicln(err)
	}
}

func readNodeFromFile(file *os.File) *node {
	var n node
	err := binary.Read(file, binary.LittleEndian, &n)
	if err != nil {
		log.Panicln(err)
	}
	return &n
}

func writeNodeToFile(pNode *node, file *os.File) {
	err := binary.Write(file, binary.LittleEndian, *pNode)
	if err != nil {
		log.Panicln(err)
	}
}

type BPlusTree struct {
	file *os.File
}

func MakeBPlusTree(file *os.File) BPlusTree {
	return BPlusTree{file}
}

var ErrKeyNotFound = errors.New("provided key not found")

func (tree BPlusTree) isFileEmpty() bool {
	info, statErr := tree.file.Stat()
	if statErr != nil {
		log.Panicln(statErr)
	}
	return info.Size() == 0
}

func (tree BPlusTree) getNextBlockAddr() AddrType {
	info, statErr := tree.file.Stat()
	if statErr != nil {
		log.Panicln(statErr)
	}
	return AddrType(info.Size())
}

func (pNode *node) String(pos AddrType) string {
	return fmt.Sprintf("addr: %d; leaf %v; p %d l %d r %d; kn %d; keys %v; chld %v", pos, pNode.Leaf, pNode.Parent, pNode.Left, pNode.Right, pNode.KeyNum, pNode.Keys[:pNode.KeyNum], pNode.Children[:pNode.KeyNum+1])
}

func (tree BPlusTree) readNodeFromFile(addr AddrType) *node {
	if addr == 0 {
		panic(addr)
	}
	if tree.isFileEmpty() {
		return nil
	}
	_, seekErr := tree.file.Seek(int64(addr), io.SeekStart)
	if seekErr != nil {
		log.Panicln(seekErr)
	}
	res := readNodeFromFile(tree.file)
	log.Printf("READ  %s", res.String(addr))
	return res
}

func (tree BPlusTree) writeNodeToFile(pNode *node, addr AddrType) {
	log.Printf("WRITE %s", pNode.String(addr))
	_, seekErr := tree.file.Seek(int64(addr), io.SeekStart)
	if seekErr != nil {
		log.Panicln(seekErr)
	}
	writeNodeToFile(pNode, tree.file)
}

func (tree BPlusTree) DFS(pos AddrType, level int) {
	pCurNode := tree.readNodeFromFile(pos)
	if pCurNode.Leaf {
		return
	}
	var i int32
	for i = 0; i <= pCurNode.KeyNum; i++ {
		tree.DFS(pCurNode.Children[i], level+1)
	}
}

func (tree BPlusTree) Find(key string) (AddrType, error) {
	var bytesKey = toKeyType(key)
	var pCurNode *node
	pHeader := tree.readHeaderFromFile()
	var nodeAddr = pHeader.Head
	pCurNode = tree.readNodeFromFile(nodeAddr)
	// find leaf
	for !pCurNode.Leaf {
		var i int32
		for ; i <= pCurNode.KeyNum; i++ {
			if i == pCurNode.KeyNum || memcmp(bytesKey, pCurNode.Keys[i]) == -1 {
				nodeAddr = pCurNode.Children[i]
				pCurNode = tree.readNodeFromFile(nodeAddr)
				break
			}
		}
	}
	// find write position in leaf
	var pos int32 = 0
	for ; pos < pCurNode.KeyNum; pos++ {
		if memcmp(bytesKey, pCurNode.Keys[pos]) == 0 {
			return pCurNode.Pointers[pos], nil
		}
	}
	return 0, ErrKeyNotFound
}

func (tree BPlusTree) split(pCurNode *node, pos AddrType) {
	pHeader := tree.readHeaderFromFile()
	for {
		midKey := pCurNode.Keys[t]
		midPointer := pCurNode.Pointers[t]
		// generate new node address
		nextAddr := tree.getNextBlockAddr()
		rightAddr := pCurNode.Right
		// update current node
		pCurNode.Right = nextAddr
		pCurNode.KeyNum = t
		tree.writeNodeToFile(pCurNode, pos)
		// bind it to right neighbour
		if rightAddr != -1 {
			pRightNode := tree.readNodeFromFile(rightAddr)
			pRightNode.Left = nextAddr
			tree.writeNodeToFile(pRightNode, rightAddr)
		}
		// create new node
		var newNode node
		newNode.Parent = pCurNode.Parent
		newNode.Left = pos
		newNode.Right = rightAddr
		newNode.KeyNum = t - 1
		copy(newNode.Keys[:newNode.KeyNum], pCurNode.Keys[t+1:])
		copy(newNode.Pointers[:newNode.KeyNum], pCurNode.Pointers[t+1:])
		copy(newNode.Children[:newNode.KeyNum+1], pCurNode.Children[t+1:])
		if pCurNode.Leaf {
			newNode.Leaf = true
			newNode.KeyNum++
			// insert mid key in a new leaf node
			copy(newNode.Keys[1:], newNode.Keys[:])
			copy(newNode.Pointers[1:], newNode.Pointers[:])
			newNode.Keys[0] = midKey
			newNode.Pointers[0] = midPointer
		} else {
			var i int32 = 0
			for ; i <= newNode.KeyNum; i++ {
				pChild := tree.readNodeFromFile(newNode.Children[i])
				pChild.Parent = nextAddr
				tree.writeNodeToFile(pChild, newNode.Children[i])
			}
		}
		tree.writeNodeToFile(&newNode, nextAddr)
		mustContinue := false
		if pos == pHeader.Head {
			// generate new address for current node and bind it to new node
			// relies on fact that root node has no left neighbour, so rebinding required only for right one == new one
			newRootAddr := tree.getNextBlockAddr()
			pHeader.Head = newRootAddr
			pCurNode.Parent = newRootAddr
			newNode.Parent = newRootAddr
			tree.writeHeaderToFile(pHeader)
			tree.writeNodeToFile(pCurNode, pos)
			tree.writeNodeToFile(&newNode, nextAddr)
			// create new root and write it
			var newRoot node
			newRoot.Parent = -1
			newRoot.Left = -1
			newRoot.Right = -1
			newRoot.KeyNum = 1
			newRoot.Keys[0] = midKey
			newRoot.Children[0] = pos
			newRoot.Children[1] = nextAddr
			tree.writeNodeToFile(&newRoot, newRootAddr)
		} else {
			pos = pCurNode.Parent
			pCurNode = tree.readNodeFromFile(pos)
			var p int32 = 0
			for ; p < pCurNode.KeyNum && memcmp(pCurNode.Keys[p], midKey) == -1; p++ {
			}
			// add midKey into node
			copy(pCurNode.Keys[p+1:], pCurNode.Keys[p:])
			copy(pCurNode.Children[p+2:], pCurNode.Children[p+1:])
			pCurNode.Keys[p] = midKey
			pCurNode.Children[p+1] = nextAddr
			pCurNode.KeyNum++
			tree.writeNodeToFile(pCurNode, pos)
			// set the flag to run another iteration
			mustContinue = pCurNode.KeyNum == 2*t
		}
		// write previous root to a new location
		if !mustContinue {
			break
		}
	}
}

func (tree BPlusTree) Insert(key string, pointer AddrType) error {
	var bytesKey = toKeyType(key)
	var pCurNode *node
	pHeader := tree.readHeaderFromFile()
	var nodeAddr = pHeader.Head
	pCurNode = tree.readNodeFromFile(nodeAddr)
	// find leaf
	for !pCurNode.Leaf {
		var i int32
		for ; i <= pCurNode.KeyNum; i++ {
			if i == pCurNode.KeyNum || memcmp(bytesKey, pCurNode.Keys[i]) == -1 {
				nodeAddr = pCurNode.Children[i]
				pCurNode = tree.readNodeFromFile(nodeAddr)
				break
			}
		}
	}
	// find write position in leaf
	var pos int32 = 0
	for ; pos < pCurNode.KeyNum; pos++ {
		cmpRes := memcmp(bytesKey, pCurNode.Keys[pos])
		if cmpRes == 0 {
			// check if key exists; only change addr value
			pCurNode.Pointers[pos] = pointer
			tree.writeNodeToFile(pCurNode, nodeAddr)
			return nil
		} else if cmpRes == -1 {
			break
		}
	}
	// shift keys and pointers to insert new value
	copy(pCurNode.Keys[pos+1:], pCurNode.Keys[pos:])
	copy(pCurNode.Pointers[pos+1:], pCurNode.Pointers[pos:])
	pCurNode.KeyNum++
	pCurNode.Keys[pos] = toKeyType(key)
	pCurNode.Pointers[pos] = pointer
	tree.writeNodeToFile(pCurNode, nodeAddr)
	// balance tree
	if pCurNode.KeyNum == 2*t {
		tree.split(pCurNode, nodeAddr)
	}
	return nil
}

func (tree BPlusTree) Init() {
	if tree.isFileEmpty() {
		var hd header
		hd.Head = -1
		tree.writeHeaderToFile(&hd)
		rootPos := tree.getNextBlockAddr()
		hd.Head = rootPos
		tree.writeHeaderToFile(&hd)
		var root node
		root.Leaf = true
		root.Parent = -1
		root.Left = -1
		root.Right = -1
		tree.writeNodeToFile(&root, rootPos)
	}
}

func (tree BPlusTree) updateKeys(nodeAddr AddrType, deletedKey KeyType, replaceKey KeyType) {
	// optimise redundant calls
	pCurNode := tree.readNodeFromFile(nodeAddr)
	for {
		nodeAddr = pCurNode.Parent
		if nodeAddr == -1 {
			break
		}
		pCurNode = tree.readNodeFromFile(nodeAddr)
		pos := tree.findKeyPos(pCurNode, deletedKey)
		if pos != -1 {
			pCurNode.Keys[pos] = replaceKey
		}
		tree.writeNodeToFile(pCurNode, nodeAddr)
		// maybe return here
	}
}

/*

DELETE:
1) find leaf
2) if nothing in leaf -> ret
3) delete key from leaf
4) if keynum >= t - 1 -> write and ret
5) else:
5.1) if pLeftNode.KeyNum > t-1 -> move item from left
5.2) else if pRightNode.KeyNum > t-1 -> move item from right
5.3) else can merge with one of them
5.3.1) if left != nil -> merge with left and delete refs from cur's parent
5.3.2) if right != nil -> merge with right one and delete refs from right's parent

*/

func (tree BPlusTree) shiftKeysLeft(pLeft *node, pRight *node) {
	pLeft.Keys[pLeft.KeyNum] = pRight.Keys[0]
	pLeft.Pointers[pLeft.KeyNum] = pRight.Pointers[0]
	pLeft.Children[pLeft.KeyNum+1] = pRight.Children[0]
	copy(pRight.Keys[:], pRight.Keys[1:])
	copy(pRight.Pointers[:], pRight.Pointers[1:])
	copy(pRight.Children[:], pRight.Children[1:])
	pLeft.KeyNum++
	pRight.KeyNum--
}

func (tree BPlusTree) shiftKeysRight(pLeft *node, pRight *node) {
	copy(pRight.Keys[1:], pRight.Keys[:])
	copy(pRight.Pointers[1:], pRight.Pointers[:])
	copy(pRight.Children[1:], pRight.Pointers[:])
	pRight.Keys[0] = pLeft.Keys[pLeft.KeyNum-1]
	pRight.Pointers[0] = pLeft.Pointers[pLeft.KeyNum-1]
	pRight.Children[0] = pLeft.Children[pLeft.KeyNum]
	pLeft.KeyNum--
	pRight.KeyNum++
}

func (tree BPlusTree) mergeNodes(pDst *node, pSrc *node) {
	copy(pDst.Keys[pDst.KeyNum:], pSrc.Keys[:])
	copy(pDst.Pointers[pDst.KeyNum:], pSrc.Pointers[:])
	copy(pDst.Children[pDst.KeyNum+1:], pSrc.Children[:])
	pDst.KeyNum += pSrc.KeyNum
}

func (tree BPlusTree) findKeyPos(pNode *node, key KeyType) int32 {
	var pos int32 = 0
	for ; pos < pNode.KeyNum; pos++ {
		if memcmp(key, pNode.Keys[pos]) == 0 {
			return pos
		}
	}
	return -1
}

func (tree BPlusTree) updateChild(pNode *node, nodeAddr AddrType) {
	pChild := tree.readNodeFromFile(pNode.Children[0])
	pChild.Parent = nodeAddr
	tree.writeNodeToFile(pChild, pNode.Children[0])
}

func (tree BPlusTree) unlinkNode(pNode *node) {
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

func (tree BPlusTree) rebindParent(pNode *node, newParent AddrType) {
	var i int32
	for ; i <= pNode.KeyNum; i++ {
		pChild := tree.readNodeFromFile(pNode.Children[i])
		pChild.Parent = newParent
		tree.writeNodeToFile(pChild, pNode.Children[i])
	}
}

func (tree BPlusTree) updatePathToRoot(nodeAddr AddrType) {
	pNode := tree.readNodeFromFile(nodeAddr)
	minKey := pNode.Keys[0]
	for pNode.Parent != -1 {
		if pNode.Left == -1 {
			return
		}
		pLeftNode := tree.readNodeFromFile(pNode.Left)
		if pLeftNode.Parent == pNode.Parent {
			pParent := tree.readNodeFromFile(pNode.Parent)
			var i int32 = 1
			for ; i <= pParent.KeyNum; i++ {
				if pParent.Children[i] == nodeAddr {
					pParent.Keys[i-1] = minKey
					tree.writeNodeToFile(pParent, pNode.Parent)
					break
				}
			}
			// write min key
			return
		}
		nodeAddr = pNode.Parent
		pNode = tree.readNodeFromFile(nodeAddr)
	}
}

func (tree BPlusTree) Delete(key string) error {
	var bytesKey = toKeyType(key)
	var pHeader = tree.readHeaderFromFile()
	var nodeAddr = pHeader.Head
	var pCurNode = tree.readNodeFromFile(nodeAddr)
	// find leaf
	for !pCurNode.Leaf {
		var i int32
		for ; i <= pCurNode.KeyNum; i++ {
			if i == pCurNode.KeyNum || memcmp(bytesKey, pCurNode.Keys[i]) == -1 {
				nodeAddr = pCurNode.Children[i]
				pCurNode = tree.readNodeFromFile(nodeAddr)
				break
			}
		}
	}
	pos := tree.findKeyPos(pCurNode, bytesKey)
	if pos == -1 {
		return ErrKeyNotFound
	}
	for {
		log.Println("HERE")
		pos = tree.findKeyPos(pCurNode, bytesKey)
		if pos == -1 {
			return nil
		}
		pCurNode.KeyNum--
		copy(pCurNode.Keys[pos:], pCurNode.Keys[pos+1:])
		copy(pCurNode.Pointers[pos:], pCurNode.Pointers[pos+1:])
		copy(pCurNode.Children[pos+1:], pCurNode.Children[pos+2:])
		if pCurNode.KeyNum >= t-1 {
			tree.updateKeys(nodeAddr, bytesKey, pCurNode.Keys[pos])
			tree.writeNodeToFile(pCurNode, nodeAddr)
			return nil
		}
		// balance tree
		var pLeftNode *node
		var pRightNode *node
		if pCurNode.Left != -1 {
			pLeftNode = tree.readNodeFromFile(pCurNode.Left)
		}
		if pCurNode.Right != -1 {
			pRightNode = tree.readNodeFromFile(pCurNode.Right)
		}
		if pLeftNode != nil && pLeftNode.KeyNum > t-1 {
			tree.shiftKeysRight(pLeftNode, pCurNode)
			if !pCurNode.Leaf {
				pChild := tree.readNodeFromFile(pCurNode.Children[0])
				pChild.Parent = nodeAddr
				tree.writeNodeToFile(pChild, pCurNode.Children[0])
			}
			tree.writeNodeToFile(pLeftNode, pCurNode.Left)
			tree.writeNodeToFile(pCurNode, nodeAddr)
			tree.updatePathToRoot(nodeAddr)
			return nil
		} else if pRightNode != nil && pRightNode.KeyNum > t-1 {
			tree.shiftKeysLeft(pCurNode, pRightNode)
			if !pCurNode.Leaf {
				pChild := tree.readNodeFromFile(pCurNode.Children[pCurNode.KeyNum-1])
				pChild.Parent = nodeAddr
				tree.writeNodeToFile(pChild, pCurNode.Children[pCurNode.KeyNum-1])
			}
			tree.writeNodeToFile(pRightNode, pCurNode.Right)
			tree.writeNodeToFile(pCurNode, nodeAddr)
			tree.updatePathToRoot(nodeAddr)
			tree.updatePathToRoot(pCurNode.Right)
			return nil
		} else {
			log.Println("MERGE")
			if pLeftNode != nil {
				if !pCurNode.Leaf {
					tree.rebindParent(pCurNode, pCurNode.Left)
				}
				tree.mergeNodes(pLeftNode, pCurNode)
				tree.writeNodeToFile(pLeftNode, pCurNode.Left)
				tree.unlinkNode(pCurNode)
				if pLeftNode.Parent == -1 {
					return nil
				}
				nodeAddr = pCurNode.Parent
				if pCurNode.KeyNum != 0 {
					bytesKey = pCurNode.Keys[0]
				}
				pCurNode = tree.readNodeFromFile(nodeAddr)
			} else if pRightNode != nil {
				if !pRightNode.Leaf {
					tree.rebindParent(pRightNode, nodeAddr)
				}
				tree.mergeNodes(pCurNode, pRightNode)
				tree.writeNodeToFile(pCurNode, nodeAddr)
				tree.unlinkNode(pRightNode)
				//tree.updateKeys(pCurNode.Left, bytesKey, pCurNode.Keys[0])
				if pCurNode.Parent == -1 {
					return nil
				}
				nodeAddr = pCurNode.Parent
				bytesKey = pRightNode.Keys[0]
				pCurNode = tree.readNodeFromFile(nodeAddr)
			} else {
				panic("WTF!?, maybe root")
			}
		}
	}
	return nil
}

// TODO: after delete insert last node to prevent fragmentation
// or maybe dispatch free blocks and tag them via bitset
// or maybe run defragmentation routine like pg_vacuum
