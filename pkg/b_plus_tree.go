package pkg

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"log"
	"os"
	"unsafe"
)

type KeyType [10]byte

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

func calculateCrc32(blob interface{}) uint32 {
	blobSize := unsafe.Sizeof(blob)
	nodeBlob := make([]byte, blobSize)
	writer := bytes.NewBuffer(nodeBlob)
	err := binary.Write(writer, binary.LittleEndian, blob)
	if err != nil {
		log.Panic(err)
	}
	checksumSize := unsafe.Sizeof(new(uint32))
	return crc32.ChecksumIEEE(nodeBlob[:blobSize-checksumSize])
}

type header struct {
	Head     AddrType
	Checksum uint32
}


type bitArray uint8

func (arr *bitArray) Set(value bool, pos int) {
	if value {
		*arr |= 1 << pos
	} else {
		bitMask := bitArray(^(1 << pos))
		*arr &= bitMask
	}
}

func (arr bitArray) Get(pos int) bool {
	return (arr >> pos) & 1 == 1
}
type nodeFlags struct {
	Flags bitArray
}

func (pFlags *nodeFlags) Leaf() bool {
	return pFlags.Flags.Get(0)
}

func (pFlags *nodeFlags) SetLeaf(value bool) {
	pFlags.Flags.Set(value, 0)
}

func (pFlags *nodeFlags) Used() bool {
	return pFlags.Flags.Get(1)
}

func (pFlags *nodeFlags) SetUsed(value bool) {
	pFlags.Flags.Set(value, 1)
}

type node struct {
	nodeFlags
	Parent   AddrType
	Left     AddrType
	Right    AddrType
	KeyNum   int32
	Keys     [2 * t]KeyType
	Pointers [2 * t]AddrType
	Children [2*t + 1]AddrType
	Checksum uint32
}

func CreateDefaultNode() *node {
	var n node
	n.SetLeaf(false)
	n.SetUsed(true)
	n.Parent = -1
	n.Left = -1
	n.Right = -1
	n.KeyNum = 0
	return &n
}

func (pHeader *header) updateChecksum() {
	pHeader.Checksum = calculateCrc32(pHeader)
}

func (pHeader *header) verifyChecksum() bool {
	return pHeader.Checksum == calculateCrc32(pHeader)
}

func (pChecksum *node) updateChecksum() {
	pChecksum.Checksum = calculateCrc32(pChecksum)
}

func (pChecksum *node) verifyChecksum() bool {
	return pChecksum.Checksum == calculateCrc32(pChecksum)
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
	if !h.verifyChecksum() {
		log.Panic("broken index: node checksum mismatch")
	}
	return &h
}

func (tree BPlusTree) writeHeaderToFile(pHeader *header) {
	_, seekErr := tree.file.Seek(0, io.SeekStart)
	if seekErr != nil {
		log.Panicln(seekErr)
	}
	pHeader.updateChecksum()
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
	if !res.verifyChecksum() {
		log.Panic("broken index: node checksum mismatch")
	}
	return res
}

func (tree BPlusTree) writeNodeToFile(pNode *node, addr AddrType) {
	_, seekErr := tree.file.Seek(int64(addr), io.SeekStart)
	if seekErr != nil {
		log.Panicln(seekErr)
	}
	pNode.updateChecksum()
	writeNodeToFile(pNode, tree.file)
}

func (tree BPlusTree) findLeafAddr(key KeyType) AddrType {
	var pCurNode *node
	pHeader := tree.readHeaderFromFile()
	var nodeAddr = pHeader.Head
	pCurNode = tree.readNodeFromFile(nodeAddr)
	// find leaf
	for !pCurNode.Leaf() {
		for i := int32(0); i <= pCurNode.KeyNum; i++ {
			if i == pCurNode.KeyNum || memcmp(key, pCurNode.Keys[i]) == -1 {
				nodeAddr = pCurNode.Children[i]
				pCurNode = tree.readNodeFromFile(nodeAddr)
				break
			}
		}
	}
	return nodeAddr
}

func (tree BPlusTree) Find(key string) (AddrType, error) {
	blobKey := toKeyType(key)
	pLeaf := tree.readNodeFromFile(tree.findLeafAddr(blobKey))
	pos := pLeaf.findKeyPos(blobKey)
	if pos == -1 {
		return 0, ErrKeyNotFound
	}
	return pLeaf.Pointers[pos], nil
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
		pNewNode := CreateDefaultNode()
		pNewNode.Parent = pCurNode.Parent
		pNewNode.Left = pos
		pNewNode.Right = rightAddr
		pNewNode.KeyNum = t - 1
		copy(pNewNode.Keys[:pNewNode.KeyNum], pCurNode.Keys[t+1:])
		copy(pNewNode.Pointers[:pNewNode.KeyNum], pCurNode.Pointers[t+1:])
		copy(pNewNode.Children[:pNewNode.KeyNum+1], pCurNode.Children[t+1:])
		if pCurNode.Leaf() {
			pNewNode.SetLeaf(true)
			pNewNode.putKey(0, midKey, midPointer)
		} else {
			tree.rebindParent(pNewNode, nextAddr)
		}
		tree.writeNodeToFile(pNewNode, nextAddr)
		mustContinue := false
		if pos == pHeader.Head {
			// generate new address for current node and bind it to new node
			// relies on fact that root node has no left neighbour, so rebinding required only for right one == new one
			newRootAddr := tree.getNextBlockAddr()
			pHeader.Head = newRootAddr
			pCurNode.Parent = newRootAddr
			pNewNode.Parent = newRootAddr
			tree.writeHeaderToFile(pHeader)
			tree.writeNodeToFile(pCurNode, pos)
			tree.writeNodeToFile(pNewNode, nextAddr)
			// create new root and write it
			pNewRoot := CreateDefaultNode()
			pNewRoot.KeyNum = 1
			pNewRoot.Keys[0] = midKey
			pNewRoot.Children[0] = pos
			pNewRoot.Children[1] = nextAddr
			tree.writeNodeToFile(pNewRoot, newRootAddr)
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

func (pNode *node) putKey(pos int32, key KeyType, pointer AddrType) {
	copy(pNode.Keys[pos+1:], pNode.Keys[pos:])
	copy(pNode.Pointers[pos+1:], pNode.Pointers[pos:])
	pNode.Keys[pos] = key
	pNode.Pointers[pos] = pointer
	pNode.KeyNum++
}

func (pNode *node) popKey(pos int32) {
	copy(pNode.Keys[pos:], pNode.Keys[pos+1:])
	copy(pNode.Pointers[pos:], pNode.Pointers[pos+1:])
	copy(pNode.Children[pos+1:], pNode.Children[pos+2:])
	pNode.KeyNum--
}

func (tree BPlusTree) Insert(key string, pointer AddrType) error {
	blobKey := toKeyType(key)
	nodeAddr := tree.findLeafAddr(blobKey)
	pLeaf := tree.readNodeFromFile(nodeAddr)
	// find write position in leaf
	var pos int32 = 0
	for ; pos < pLeaf.KeyNum; pos++ {
		cmpRes := memcmp(blobKey, pLeaf.Keys[pos])
		if cmpRes == 0 {
			// check if key exists; only change addr value
			pLeaf.Pointers[pos] = pointer
			tree.writeNodeToFile(pLeaf, nodeAddr)
			return nil
		} else if cmpRes == -1 {
			break
		}
	}
	pLeaf.putKey(pos, blobKey, nodeAddr)
	tree.writeNodeToFile(pLeaf, nodeAddr)
	// balance tree
	if pLeaf.KeyNum == 2*t {
		tree.split(pLeaf, nodeAddr)
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
		pRoot := CreateDefaultNode()
		pRoot.SetLeaf(true)
		tree.writeNodeToFile(pRoot, rootPos)
	}
}

func (tree BPlusTree) shiftKeysLeft(pLeft *node, pRight *node) {
	pLeft.Keys[pLeft.KeyNum] = pRight.Keys[0]
	pLeft.Children[pLeft.KeyNum+1] = pRight.Children[0]
	copy(pRight.Keys[:], pRight.Keys[1:])
	copy(pRight.Children[:], pRight.Children[1:])
	pLeft.KeyNum++
	pRight.KeyNum--
}

func (tree BPlusTree) shiftKeysRight(pLeft *node, pRight *node) {
	copy(pRight.Keys[1:], pRight.Keys[:])
	copy(pRight.Children[1:], pRight.Children[:])
	pRight.Keys[0] = pLeft.Keys[pLeft.KeyNum-1]
	pRight.Children[0] = pLeft.Children[pLeft.KeyNum]
	pLeft.KeyNum--
	pRight.KeyNum++
}

func (tree BPlusTree) mergeNodes(pDst *node, pSrc *node) {
	copy(pDst.Keys[pDst.KeyNum:], pSrc.Keys[:])
	copy(pDst.Pointers[pDst.KeyNum:], pSrc.Pointers[:])
	pDst.KeyNum += pSrc.KeyNum
}

func (tree BPlusTree) mergeInternalNodes(pDst *node, pSrc *node) {
	pChild := tree.readNodeFromFile(tree.findMinLeaf(pSrc.Children[0]))
	pDst.Keys[pDst.KeyNum] = pChild.Keys[0]
	pDst.Children[pDst.KeyNum+1] = pSrc.Children[0]
	pDst.KeyNum++
	copy(pDst.Keys[pDst.KeyNum:], pSrc.Keys[:])
	copy(pDst.Children[pDst.KeyNum+1:], pSrc.Children[1:])
	pDst.KeyNum += pSrc.KeyNum
}

func (pNode *node) findKeyPos(key KeyType) int32 {
	for pos := int32(0); pos < pNode.KeyNum; pos++ {
		if memcmp(key, pNode.Keys[pos]) == 0 {
			return pos
		}
	}
	return -1
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
	for i := int32(0); i <= pNode.KeyNum; i++ {
		pChild := tree.readNodeFromFile(pNode.Children[i])
		pChild.Parent = newParent
		tree.writeNodeToFile(pChild, pNode.Children[i])
	}
}

func (tree BPlusTree) findMinLeaf(nodeAddr AddrType) AddrType {
	pNode := tree.readNodeFromFile(nodeAddr)
	for !pNode.Leaf() {
		nodeAddr = pNode.Children[0]
		pNode = tree.readNodeFromFile(nodeAddr)
	}
	return nodeAddr
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
			for i := int32(0); i <= pParent.KeyNum; i++ {
				if pParent.Children[i] == nodeAddr {
					pParent.Keys[i-1] = minKey
					tree.writeNodeToFile(pParent, pNode.Parent)
					break
				}
			}
			return
		}
		nodeAddr = pNode.Parent
		pNode = tree.readNodeFromFile(nodeAddr)
	}
}

func (tree BPlusTree) deleteInternal(nodeAddr AddrType, key KeyType, removeFirst bool) {
	for {
		var pCurNode = tree.readNodeFromFile(nodeAddr)
		if removeFirst {
			copy(pCurNode.Keys[0:], pCurNode.Keys[1:])
			copy(pCurNode.Children[0:], pCurNode.Children[1:])
			pCurNode.KeyNum--
			tree.writeNodeToFile(pCurNode, nodeAddr)
			tree.updatePathToRoot(tree.findMinLeaf(nodeAddr))
		} else {
			pos := pCurNode.findKeyPos(key)
			if pos == -1 {
				return
			}
			pCurNode.popKey(pos)
			tree.writeNodeToFile(pCurNode, nodeAddr)
		}
		removeFirst = false
		if pCurNode.KeyNum >= t-1 {
			return
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
			pChild := tree.readNodeFromFile(pCurNode.Children[0])
			pChild.Parent = nodeAddr
			tree.writeNodeToFile(pChild, pCurNode.Children[0])
			tree.writeNodeToFile(pLeftNode, pCurNode.Left)
			tree.writeNodeToFile(pCurNode, nodeAddr)
			tree.updatePathToRoot(tree.findMinLeaf(pCurNode.Children[0]))
			tree.updatePathToRoot(tree.findMinLeaf(pCurNode.Children[1]))
			tree.updatePathToRoot(tree.findMinLeaf(pCurNode.Left))
			return
		} else if pRightNode != nil && pRightNode.KeyNum > t-1 {
			tree.shiftKeysLeft(pCurNode, pRightNode)
			pChild := tree.readNodeFromFile(pCurNode.Children[pCurNode.KeyNum])
			pChild.Parent = nodeAddr
			tree.writeNodeToFile(pChild, pCurNode.Children[pCurNode.KeyNum])
			tree.writeNodeToFile(pCurNode, nodeAddr)
			tree.writeNodeToFile(pRightNode, pCurNode.Right)
			tree.updatePathToRoot(tree.findMinLeaf(pCurNode.Children[pCurNode.KeyNum]))
			tree.updatePathToRoot(tree.findMinLeaf(pCurNode.Right))
			return
		} else {
			if pLeftNode != nil {
				tree.rebindParent(pCurNode, pCurNode.Left)
				tree.mergeInternalNodes(pLeftNode, pCurNode)
				pCurNode.SetUsed(false)
				tree.writeNodeToFile(pCurNode, nodeAddr)
				tree.writeNodeToFile(pLeftNode, pCurNode.Left)
				tree.unlinkNode(pCurNode)
				if pCurNode.Parent == -1 {
					return
				}
				key = tree.readNodeFromFile(tree.findMinLeaf(nodeAddr)).Keys[0]
				nodeAddr = pCurNode.Parent
				removeFirst = pLeftNode.Parent != pCurNode.Parent
			} else if pRightNode != nil {
				tree.rebindParent(pRightNode, nodeAddr)
				tree.mergeInternalNodes(pCurNode, pRightNode)
				pRightNode.SetUsed(false)
				tree.writeNodeToFile(pRightNode, pCurNode.Right)
				tree.writeNodeToFile(pCurNode, nodeAddr)
				tree.unlinkNode(pRightNode)
				if pCurNode.Parent == -1 {
					return
				}
				nodeAddr = pCurNode.Parent
				key = tree.readNodeFromFile(tree.findMinLeaf(pCurNode.Right)).Keys[0]
			} else {
				// root deletion case
				pHeader := tree.readHeaderFromFile()
				pRoot := tree.readNodeFromFile(pHeader.Head)
				if pRoot.KeyNum == 0 {
					pRoot.SetUsed(false)
					tree.writeNodeToFile(pRoot, pHeader.Head)
					pHeader.Head = pCurNode.Children[0]
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
	blobKey := toKeyType(key)
	nodeAddr := tree.findLeafAddr(blobKey)
	pLeaf := tree.readNodeFromFile(nodeAddr)
	pos := pLeaf.findKeyPos(blobKey)
	if pos == -1 {
		return ErrKeyNotFound
	}
	pLeaf.popKey(pos)
	tree.writeNodeToFile(pLeaf, nodeAddr)
	tree.updatePathToRoot(nodeAddr)
	if pLeaf.KeyNum >= t-1 {
		return nil
	}
	// balance tree
	var pLeftNode *node
	var pRightNode *node
	if pLeaf.Left != -1 {
		pLeftNode = tree.readNodeFromFile(pLeaf.Left)
	}
	if pLeaf.Right != -1 {
		pRightNode = tree.readNodeFromFile(pLeaf.Right)
	}
	if pLeftNode != nil && pLeftNode.KeyNum > t-1 {
		tree.shiftKeysRight(pLeftNode, pLeaf)
		tree.writeNodeToFile(pLeftNode, pLeaf.Left)
		tree.writeNodeToFile(pLeaf, nodeAddr)
		tree.updatePathToRoot(nodeAddr)
	} else if pRightNode != nil && pRightNode.KeyNum > t-1 {
		tree.shiftKeysLeft(pLeaf, pRightNode)
		tree.writeNodeToFile(pRightNode, pLeaf.Right)
		tree.writeNodeToFile(pLeaf, nodeAddr)
		tree.updatePathToRoot(nodeAddr)
		tree.updatePathToRoot(pLeaf.Right)
	} else {
		if pLeftNode != nil {
			tree.mergeNodes(pLeftNode, pLeaf)
			pLeaf.SetUsed(false)
			tree.writeNodeToFile(pLeaf, nodeAddr)
			tree.writeNodeToFile(pLeftNode, pLeaf.Left)
			tree.unlinkNode(pLeaf)
			tree.updatePathToRoot(nodeAddr)
			tree.deleteInternal(pLeaf.Parent, pLeaf.Keys[0], pLeftNode.Parent != pLeaf.Parent)
		} else if pRightNode != nil {
			tree.mergeNodes(pLeaf, pRightNode)
			pRightNode.SetUsed(false)
			tree.writeNodeToFile(pRightNode, pLeaf.Right)
			tree.writeNodeToFile(pLeaf, nodeAddr)
			tree.unlinkNode(pRightNode)
			tree.updatePathToRoot(nodeAddr)
			tree.deleteInternal(pLeaf.Parent, pRightNode.Keys[0], false)
		}
	}
	return nil
}

// TODO: after delete insert last node to prevent fragmentation
// or maybe dispatch free blocks and tag them via bitset
// or maybe run defragmentation routine like pg_vacuum
