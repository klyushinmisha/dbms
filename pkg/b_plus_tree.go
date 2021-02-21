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
		nodeAddr := pCurNode.Parent
		if nodeAddr == -1 {
			break
		}
		pCurNode = tree.readNodeFromFile(nodeAddr)
		var i int32
		for ; i < pCurNode.KeyNum; i++ {
			if memcmp(pCurNode.Keys[i], deletedKey) == 0 {
				pCurNode.Keys[i] = replaceKey
				break
			}
		}
		tree.writeNodeToFile(pCurNode, nodeAddr)
		// maybe return here
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
		log.Println(pCurNode.Leaf)
		deletePos = -1
		for ; pos < pCurNode.KeyNum; pos++ {
			if memcmp(bytesKey, pCurNode.Keys[pos]) == 0 {
				deletePos = pos
				break
			}
		}
		if deletePos == -1 {
			return nil
		}
		copy(pCurNode.Keys[pos:], pCurNode.Keys[pos+1:])
		copy(pCurNode.Pointers[pos:], pCurNode.Pointers[pos+1:])
		copy(pCurNode.Children[pos+1:], pCurNode.Children[pos+2:])
		replaceKey := pCurNode.Keys[pos]
		pCurNode.KeyNum--
		mustContinue := false
		if pCurNode.KeyNum < t-1 {
			log.Println("1")
			var pLeftNode *node
			var pRightNode *node
			if pCurNode.Left != -1 {
				pLeftNode = tree.readNodeFromFile(pCurNode.Left)
			}
			if pCurNode.Right != -1 {
				pRightNode = tree.readNodeFromFile(pCurNode.Right)
			}
			if pLeftNode != nil && pLeftNode.KeyNum > t-1 {
				log.Println("1.1")
				pCurNode.KeyNum++
				pLeftNode.KeyNum--
				tree.writeNodeToFile(pLeftNode, pCurNode.Left)
				copy(pCurNode.Keys[1:], pCurNode.Keys[:])
				copy(pCurNode.Pointers[1:], pCurNode.Pointers[:])
				copy(pCurNode.Children[1:], pCurNode.Children[:])
				pCurNode.Keys[0] = pLeftNode.Keys[pLeftNode.KeyNum]
				pCurNode.Pointers[0] = pLeftNode.Pointers[pLeftNode.KeyNum]
				pCurNode.Children[0] = pLeftNode.Children[pLeftNode.KeyNum+1]
				if !pCurNode.Leaf {
					pChild := tree.readNodeFromFile(pCurNode.Children[0])
					pChild.Parent = nodeAddr
					tree.writeNodeToFile(pChild, pCurNode.Children[0])
				}
				tree.writeNodeToFile(pCurNode, nodeAddr)
				// update keys on the way to the root
				tree.updateKeys(nodeAddr, bytesKey, pCurNode.Keys[0])
			} else if pRightNode != nil && pRightNode.KeyNum > t-1 {
				log.Println("1.2")
				pCurNode.KeyNum++
				pRightNode.KeyNum--
				pCurNode.Keys[pCurNode.KeyNum-1] = pRightNode.Keys[0]
				pCurNode.Pointers[pCurNode.KeyNum-1] = pRightNode.Pointers[0]
				pCurNode.Children[pCurNode.KeyNum] = pRightNode.Children[0]
				if !pCurNode.Leaf {
					pChild := tree.readNodeFromFile(pCurNode.Children[pCurNode.KeyNum])
					pChild.Parent = nodeAddr
					tree.writeNodeToFile(pChild, pCurNode.Children[pCurNode.KeyNum])
				}
				copy(pRightNode.Keys[:], pRightNode.Keys[1:])
				copy(pRightNode.Pointers[:], pRightNode.Pointers[1:])
				copy(pRightNode.Children[:], pRightNode.Children[1:])
				tree.writeNodeToFile(pRightNode, pCurNode.Right)
				tree.writeNodeToFile(pCurNode, nodeAddr)
				// update keys on the way to the root
				// тут проблема с тем, что нужно обновить родительский узел информацией из родителя
				tree.updateKeys(nodeAddr, bytesKey, pCurNode.Keys[0])
				if pRightNode.Parent != -1 {
					pParent := tree.readNodeFromFile(pRightNode.Parent)
					var i int32
					for ; i < pParent.KeyNum; i++ {
						if memcmp(pParent.Keys[i], pCurNode.Keys[pCurNode.KeyNum-1]) == 0 {
							pParent.Keys[i] = pRightNode.Keys[0]
							break
						}
					}
					tree.writeNodeToFile(pParent, pRightNode.Parent)
				}
			} else {
				log.Println("1.3")
				// теперь баг в этой ветке
				if pLeftNode != nil {
					log.Println("1.3.1")
					// merge current node with left one
					if !pCurNode.Leaf {
						var i int32
						for ; i < pCurNode.KeyNum; i++ {
							pChild := tree.readNodeFromFile(pCurNode.Children[i])
							pChild.Parent = pCurNode.Left
							tree.writeNodeToFile(pChild, pCurNode.Children[i])
						}
					}
					copy(pLeftNode.Keys[pLeftNode.KeyNum:], pCurNode.Keys[:])
					copy(pLeftNode.Pointers[pLeftNode.KeyNum:], pCurNode.Pointers[:])
					copy(pLeftNode.Children[pLeftNode.KeyNum:], pCurNode.Children[:])
					pLeftNode.KeyNum += pCurNode.KeyNum
					// update keys on the way to the root
					pLeftNode.Right = pCurNode.Right
					tree.writeNodeToFile(pLeftNode, pCurNode.Left)
					if pRightNode != nil {
						pRightNode.Left = pCurNode.Left
						tree.writeNodeToFile(pRightNode, pCurNode.Right)
					}
					bytesKey = pCurNode.Keys[0]
					// delete pCurNode refs from parent
					tree.updateKeys(pCurNode.Left, bytesKey, replaceKey)
					pCurNode = tree.readNodeFromFile(pLeftNode.Parent)
					mustContinue = true
				} else if pRightNode != nil {
					log.Println("1.3.2")
					// merge current node with right one
					if !pRightNode.Leaf {
						var i int32
						for ; i < pRightNode.KeyNum; i++ {
							pChild := tree.readNodeFromFile(pRightNode.Children[i])
							pChild.Parent = nodeAddr
							tree.writeNodeToFile(pChild, pRightNode.Children[i])
						}
					}
					copy(pCurNode.Keys[pCurNode.KeyNum:], pRightNode.Keys[:])
					copy(pCurNode.Pointers[pCurNode.KeyNum:], pRightNode.Pointers[:])
					copy(pCurNode.Children[pCurNode.KeyNum:], pRightNode.Children[:])
					pCurNode.KeyNum += pRightNode.KeyNum
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
					tree.writeNodeToFile(pCurNode, nodeAddr)
					tree.updateKeys(nodeAddr, bytesKey, replaceKey)
					bytesKey = pRightNode.Keys[0]
					nodeAddr = pCurNode.Parent
					pCurNode = tree.readNodeFromFile(nodeAddr)
					mustContinue = true
				} else {
					log.Println("1.3.3")
					tree.writeNodeToFile(pCurNode, nodeAddr)
					tree.updateKeys(nodeAddr, bytesKey, replaceKey)
				}
			}
		} else {
			log.Println("1.3.4")
			tree.writeNodeToFile(pCurNode, nodeAddr)
			tree.updateKeys(nodeAddr, bytesKey, replaceKey)
		}
		if !mustContinue {
			break
		}
	}
	return nil
}

// TODO: after delete insert last node to prevent fragmentation
// or maybe dispatch free blocks and tag them via bitset
// or maybe run defragmentation routine like pg_vacuum
