package pkg

import (
	"encoding/binary"
	"errors"
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

func (tree BPlusTree) readNodeFromFile(addr AddrType) *node {
	if tree.isFileEmpty() {
		return nil
	}
	_, seekErr := tree.file.Seek(int64(addr), io.SeekStart)
	if seekErr != nil {
		log.Panicln(seekErr)
	}
	return readNodeFromFile(tree.file)
}

func (tree BPlusTree) writeNodeToFile(pNode *node, addr AddrType) {
	_, seekErr := tree.file.Seek(int64(addr), io.SeekStart)
	if seekErr != nil {
		log.Panicln(seekErr)
	}
	writeNodeToFile(pNode, tree.file)
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
		// save midKey and midPointer for future use
		midKey := pCurNode.Keys[t]
		midPointer := pCurNode.Pointers[t]
		// generate new node address
		nextAddr := tree.getNextBlockAddr()
		// bind it to right neighbour
		if pCurNode.Right != -1 {
			pRightNode := tree.readNodeFromFile(pCurNode.Right)
			pRightNode.Left = nextAddr
			tree.writeNodeToFile(pRightNode, pCurNode.Right)
		}
		// create new node
		var newNode node
		newNode.Parent = pCurNode.Parent
		newNode.Left = pos
		newNode.Right = pCurNode.Right
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
		}
		tree.writeNodeToFile(&newNode, nextAddr)
		// update current node
		pCurNode.Right = nextAddr
		pCurNode.KeyNum = t
		mustContinue := false
		tree.writeNodeToFile(pCurNode, pos)
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

func (tree BPlusTree) Delete(key string) error {
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
	// find item to delete in leaf
	var pos int32 = 0
	var deletePos int32 = -1
	for ; pos < pCurNode.KeyNum; pos++ {
		if memcmp(bytesKey, pCurNode.Keys[pos]) == 0 {
			deletePos = pos
			break
		}
	}
	if deletePos == -1 {
		return ErrKeyNotFound
	}

	// recursively delete items
	for {
		pos = 0
		for ; pos < pCurNode.KeyNum && memcmp(bytesKey, pCurNode.Keys[pos]) != 0; pos++ {
		}
		copy(pCurNode.Keys[pos:], pCurNode.Keys[pos+1:])
		copy(pCurNode.Pointers[pos:], pCurNode.Pointers[pos+1:])
		copy(pCurNode.Children[pos+1:], pCurNode.Children[pos+2:])
		pCurNode.KeyNum--
		mustContinue := false
		if pCurNode.KeyNum < t-1 {
			var pLeftNode *node
			var pRightNode *node
			if pCurNode.Left != -1 {
				pLeftNode = tree.readNodeFromFile(pCurNode.Left)
			}
			if pCurNode.Right != -1 {
				pRightNode = tree.readNodeFromFile(pCurNode.Right)
			}
			if pLeftNode != nil && pLeftNode.KeyNum > t-1 {
				pCurNode.KeyNum++
				pLeftNode.KeyNum--
				tree.writeNodeToFile(pLeftNode, pCurNode.Left)
				copy(pCurNode.Keys[1:], pCurNode.Keys[:])
				copy(pCurNode.Pointers[1:], pCurNode.Pointers[:])
				copy(pCurNode.Children[1:], pCurNode.Children[:])
				pCurNode.Keys[0] = pLeftNode.Keys[pLeftNode.KeyNum]
				pCurNode.Pointers[0] = pLeftNode.Pointers[pLeftNode.KeyNum]
				pCurNode.Children[0] = pLeftNode.Children[pLeftNode.KeyNum+1]
				// update keys on the way to the root
			} else if pRightNode != nil && pRightNode.KeyNum > t-1 {
				pCurNode.KeyNum++
				pRightNode.KeyNum--
				pCurNode.Keys[pCurNode.KeyNum-1] = pRightNode.Keys[0]
				pCurNode.Pointers[pCurNode.KeyNum-1] = pRightNode.Pointers[0]
				pCurNode.Children[pCurNode.KeyNum] = pRightNode.Children[0]
				copy(pRightNode.Keys[:], pRightNode.Keys[1:])
				copy(pRightNode.Pointers[:], pRightNode.Pointers[1:])
				copy(pRightNode.Children[:], pRightNode.Children[1:])
				tree.writeNodeToFile(pRightNode, pCurNode.Right)
				// update keys on the way to the root
			} else {
				if pLeftNode != nil {
					// merge current node with left one
					copy(pLeftNode.Keys[:], pLeftNode.Keys[1:])
					copy(pLeftNode.Pointers[:], pLeftNode.Pointers[1:])
					copy(pLeftNode.Children[:], pLeftNode.Children[1:])
					// update keys on the way to the root
					pLeftNode.Right = pCurNode.Right
					tree.writeNodeToFile(pLeftNode, pCurNode.Left)
					if pRightNode != nil {
						pRightNode.Left = pCurNode.Left
						tree.writeNodeToFile(pRightNode, pCurNode.Right)
					}
					bytesKey = pCurNode.Keys[0]
					pCurNode = tree.readNodeFromFile(pLeftNode.Parent)
					mustContinue = true
				} else if pRightNode != nil {
					// merge current node with right one
					copy(pRightNode.Keys[:], pRightNode.Keys[1:])
					copy(pRightNode.Pointers[:], pRightNode.Pointers[1:])
					copy(pRightNode.Children[:], pRightNode.Children[1:])
					// update keys on the way to the root
					var pRightRightNode *node
					if pRightNode.Right != -1 {
						pRightRightNode = tree.readNodeFromFile(pRightNode.Right)
					}
					if pRightRightNode != nil {
						pRightRightNode.Left = nodeAddr
						tree.writeNodeToFile(pRightRightNode, pRightNode.Right)
					}
					pCurNode.Right = pRightNode.Right
					bytesKey = pRightNode.Keys[0]
					pCurNode = tree.readNodeFromFile(pCurNode.Parent)
					mustContinue = true
				}
			}
		}
		tree.writeNodeToFile(pCurNode, nodeAddr)
		if !mustContinue {
			break
		}
	}
	return nil
}

// TODO: after delete insert last node to prevent fragmentation
// or maybe dispatch free blocks and tag them via bitset
// or maybe run defragmentation routine like pg_vacuum
