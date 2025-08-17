package main

import (
    "bytes"
    "encoding/binary"
    "fmt"
)

/* Set Up */
func assert(cond bool) {
    if !cond {
        panic("assertion failed")
    }
}

type BNode struct {
	data []byte // content stored as raw bytes, can be dumped to the disk
}

const (
	BNODE_NODE = 1 // internal nodes without values, contain key & values
	BNODE_LEAF = 2 // leaf nodes with values
)

type BTree struct {
	// pointer (a nonzero page number)
	root uint64

	// callbacks for managing on-disk pages
	get func(uint64) BNode // dereference a pointer; fetch page by page number
	new func(BNode) uint64 // allocate a new page with given node data
	del func(uint64) // deallocate a page by page number
}

const HEADER = 4 // metadata
const BTREE_PAGE_SIZE = 4096
const BTREE_MAX_KEY_SIZE = 1000
const BTREE_MAX_VAL_SIZE = 3000

func init() {
	node1max := HEADER + 8 + 2 + 4 + BTREE_MAX_KEY_SIZE + BTREE_MAX_VAL_SIZE // 8: single child pointer, 2: type field size, 4: nkeys & offsets?
	assert(node1max <= BTREE_PAGE_SIZE)
}

// header
// Bytes [0:2) store the node type (2 bytes, uint16).
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node.data)
}
// Bytes [2:4) store the number of keys (nkeys) in the node (2 bytes, uint16).
func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node.data[2:4])
}
func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node.data[0:2], btype)
	binary.LittleEndian.PutUint16(node.data[2:4], nkeys)
}

// pointers
// After the 4-byte header, starting at offset HEADER = 4, there is an array of pointers.
// Each pointer is 8 bytes (uint64) - page number
func (node BNode) getPtr(idx uint16) uint64 {
	assert(idx < node.nkeys())
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node.data[pos:])
}
func (node BNode) setPtr(idx uint16, val uint64) {
	assert(idx < node.nkeys())
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node.data[pos:], val)
}

// offset list
// allow direct addressing of any KV pair without scanning all previous pairs; provide indexing pointers
// each offset corresponds to a key-value pair index (starting from 1)
// offset starts after pointers array (4 + nkeys*8), 2 bytes each, idx is 1-based
func offsetPos(node BNode, idx uint16) uint16 {
	assert(1 <= idx && idx <= node.nkeys())
	return HEADER + 8*node.nkeys() + 2*(idx-1)
}
func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}
	return binary.LittleEndian.Uint16(node.data[offsetPos(node, idx):])
}
func (node BNode) setOffset(idx uint16, offset uint16) {
	binary.LittleEndian.PutUint16(node.data[offsetPos(node, idx):], offset)
}

// key-values
// starts after 4 + nkeys8 + nkeys2, variable size
func (node BNode) kvPos(idx uint16) uint16 {
	assert(idx <= node.nkeys())
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}
func (node BNode) getKey(idx uint16) []byte {
	assert(idx < node.nkeys())
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	return node.data[pos+4:][:klen]
}
func (node BNode) getVal(idx uint16) []byte {
	assert(idx < node.nkeys())
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos+0:])
	vlen := binary.LittleEndian.Uint16(node.data[pos+2:])
	return node.data[pos+4+klen:][:vlen]
}

// node size in bytes
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

/* B-Tree Insertion */

// returns the first kid node whose range intersects the key. (kid[i] <= key)
// TODO: bisect
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()
	if nkeys == 0 {
        return 0
    }
	found := uint16(0)
	// the first key is a copy from the parent node,
	// thus it's always less than or equal to the key
	for i := uint16(1); i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp <= 0 {
			found = i
		}
		if cmp >= 0 {
			break
		}
	}
	return found
}

// Bisect implementation
// func nodeLookupLE(node BNode, key []byte) uint16 {
//     nkeys := node.nkeys()
//     low, high := uint16(1), nkeys - 1
//     found := uint16(0) // default to 0 (the first kid)

//     for low <= high {
//         mid := low + (high - low) / 2
//         cmp := bytes.Compare(node.getKey(mid), key)
//         if cmp <= 0 {
//             found = mid  // key at mid <= input key, candidate found, search right for larger index
//             low = mid + 1
//         } else {
//             // key at mid > input key, search left
//             high = mid - 1
//         }
//     }

//     return found
// }

// Update Leaf Nodes
func leafInsert(
	new BNode, old BNode, idx uint16, key []byte, val []byte,
){
	new.setHeader(BNODE_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)
}

// copy multiple KVs into the position; copies keys from an old node to new node
func nodeAppendRange(
	new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16,
) {
	assert(srcOld+n <= old.nkeys())
	assert(dstNew+n <= new.nkeys())
	if n == 0 {
		return
	}

	// pointers
	for i := uint16(0); i < n; i++ {
		new.setPtr(dstNew+i, old.getPtr(srcOld+i))
	}

	// offsets
	dstBegin := new.getOffset(dstNew)
	srcBegin := old.getOffset(srcOld)
	for i := uint16(1); i <= n; i++ { // Note: the range is [1, n]
		offset := dstBegin + old.getOffset(srcOld+i) - srcBegin
		new.setOffset(dstNew+i, offset)
	}

	// KVs
	begin := old.kvPos(srcOld)
	end := old.kvPos(srcOld + n)
	copy(new.data[new.kvPos(dstNew):], old.data[begin:end])
}

// copies a KV pair to the new node
func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	// ptrs
	new.setPtr(idx, ptr)
	// KVs
	pos := new.kvPos(idx)
	binary.LittleEndian.PutUint16(new.data[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new.data[pos+2:], uint16(len(val)))
	copy(new.data[pos+4:], key)
	copy(new.data[pos+4+uint16(len(key)):], val)
	// the offset of the next key
	new.setOffset(idx+1, new.getOffset(idx)+4+uint16((len(key)+len(val))))
}

// main function for inserting a key
// insert a KV into a node, the result might be split into 2 nodes.
// the caller is responsible for deallocating the input node and splitting and allocating result nodes.
func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	// the result node. allowed to be bigger than 1 page and will be split if so
	new := BNode {data: make([]byte, 2*BTREE_PAGE_SIZE)}

	nkeys := node.nkeys()
	
	// If the node has no keys yet, insert at position 0 directly
	if nkeys == 0 {
		if node.btype() == BNODE_LEAF {
			// insert first key-value pair into empty leaf
			leafInsert(new, node, 0, key, val)
			return new
		} else if node.btype() == BNODE_NODE {
			// For internal nodes with zero keys (rare), just delegate to nodeInsert at 0
			nodeInsert(tree, new, node, 0, key, val)
			return new
		} else {
			panic("bad node!")
		}
	}

	// where to insert the key?
	idx := nodeLookupLE(node, key)
	// act depending on the node type
	switch node.btype() {
	case BNODE_LEAF:
		// leaf, node.getKey(idx) <= key
		if bytes.Equal(key, node.getKey(idx)) {
			// found the key, update it.
			leafUpdate(new, node, idx, key, val)
		} else {
			// insert it after the position	
			leafInsert(new, node, idx+1, key, val)
		}
	case BNODE_NODE:
		// internal node, insert it to a kid node.
		nodeInsert(tree, new, node, idx, key, val)
	default:
		panic("bad node!")
	}
	return new
}

// TODO: leafUpdate
func leafUpdate(
	new BNode, old BNode, idx uint16, key []byte, val []byte,
) {
	// Number of keys remains the same
	nkeys := old.nkeys()
	new.setHeader(BNODE_LEAF, nkeys)

	// Copy all keys and values before idx (exclusive)
	if idx > 0 {
		nodeAppendRange(new, old, 0, 0, idx)
	}

	// Replace KV at idx with new key and value, pointer is 0 for leaf
	nodeAppendKV(new, idx, 0, key, val)

	// Copy all keys and values after idx (exclusive)
	if idx+1 < nkeys {
		nodeAppendRange(new, old, idx+1, idx+1, nkeys-(idx+1))
	}
}

// part of the treeInsert(): KV insertion to an internal node
func nodeInsert(
	tree *BTree, new BNode, node BNode, idx uint16, key []byte, val []byte,
){
	// get and deallocate the kid node
	kptr := node.getPtr(idx)
	knode := tree.get(kptr)
	tree.del(kptr)
	// recursive insertion to the kid node
	knode = treeInsert(tree, knode, key, val)
	// split the result
	nsplit, splited := nodeSplit3(knode)
	// update the kid links
	nodeReplaceKidN(tree, new, node, idx, splited[:nsplit]...) // variadic unpacking
}

// Split Big Nodes
func nodeSplit2(left BNode, right BNode, old BNode) {
	nkeys := old.nkeys()
	// Split roughly in half, leaning towards left having more keys if odd
	splitIdx := nkeys / 2
	if splitIdx == 0 {
		splitIdx = 1
	}

	btype := old.btype()

	// Set headers for new nodes
	left.setHeader(btype, splitIdx)
	right.setHeader(btype, nkeys-splitIdx)

	// Copy keys and pointers to left node [0..splitIdx-1]
	nodeAppendRange(left, old, 0, 0, splitIdx)

	// Copy keys and pointers to right node [splitIdx..nkeys-1]
	nodeAppendRange(right, old, 0, splitIdx, nkeys-splitIdx)

	// For internal nodes, ensure the first key in right is the separator
	// (usually the first key of the right node is copied up)
	// This depends on your B-tree implementation details.

	// Slice data arrays to max page size to avoid extra bytes
	left.data = left.data[:BTREE_PAGE_SIZE]
	right.data = right.data[:BTREE_PAGE_SIZE]
}

// split a node if it's too big. the results are 1~3 nodes.
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old.data = old.data[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old}
	}

	left := BNode{make([]byte, 2*BTREE_PAGE_SIZE)} // might be split later
	right := BNode{make([]byte, BTREE_PAGE_SIZE)}
	nodeSplit2(left, right, old)

	if left.nbytes() <= BTREE_PAGE_SIZE {
		left.data = left.data[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right}
	}

	// the left node is still too large, split again
	leftleft := BNode{make([]byte, BTREE_PAGE_SIZE)}
	middle := BNode{make([]byte, BTREE_PAGE_SIZE)}
	nodeSplit2(leftleft, middle, left)

	assert(leftleft.nbytes() <= BTREE_PAGE_SIZE)

	return 3, [3]BNode{leftleft, middle, right}
}

// replace a link with multiple links 
func nodeReplaceKidN(
	tree *BTree, new BNode, old BNode, idx uint16, kids ...BNode,
) {
	inc := uint16(len(kids))
	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)
	for i, node := range kids {
		nodeAppendKV(new, idx+uint16(i), tree.new(node), node.getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}

// /* Visualise */
// func printNode(tree *BTree, node BNode, level int) {
// 	indent := ""
// 	for i := 0; i < level; i++ {
// 		indent += "  "
// 	}

// 	nkeys := node.nkeys()
// 	nodeType := "Internal"
// 	if node.btype() == BNODE_LEAF {
// 		nodeType = "Leaf"
// 	}

// 	fmt.Printf("%s%s Node: nkeys=%d\n", indent, nodeType, nkeys)

// 	if nkeys == 0 {
// 		fmt.Printf("%s  (empty node)\n", indent)
// 		return
// 	}

// 	for i := uint16(0); i < nkeys; i++ {
// 		key := node.getKey(i)
// 		if node.btype() == BNODE_LEAF {
// 			val := node.getVal(i)
// 			fmt.Printf("%s  [%d] Key: %q, Value: %q\n", indent, i, key, val)
// 		} else {
// 			ptr := node.getPtr(i)
// 			fmt.Printf("%s  [%d] Key: %q, Ptr: %d\n", indent, i, key, ptr)
// 		}
// 	}

// 	// For internal nodes, recursively print children (nkeys + 1 pointers)
// 	if node.btype() == BNODE_NODE {
// 		for i := uint16(0); i <= nkeys; i++ {
// 			ptr := node.getPtr(i)
// 			if ptr == 0 {
// 				continue
// 			}
// 			childNode := tree.get(ptr)
// 			printNode(tree, childNode, level+1)
// 		}
// 	}
// }


/* Test */
type InMemoryBTree struct {
    pages map[uint64]BNode
    nextPage uint64
}

func NewInMemoryBTree() *BTree {
    mem := &InMemoryBTree{
        pages: make(map[uint64]BNode),
        nextPage: 1,
    }
    tree := &BTree{
        root: 0,
        get: mem.get,
        new: mem.new,
        del: mem.del,
    }
    return tree
}

func (mem *InMemoryBTree) get(page uint64) BNode {
    return mem.pages[page]
}

func (mem *InMemoryBTree) new(node BNode) uint64 {
    page := mem.nextPage
    mem.nextPage++
    mem.pages[page] = node
    return page
}

func (mem *InMemoryBTree) del(page uint64) {
    delete(mem.pages, page)
}

func main() {
    tree := NewInMemoryBTree()

    // Create root leaf node with no keys
    rootNode := BNode{data: make([]byte, BTREE_PAGE_SIZE)}
    rootNode.setHeader(BNODE_LEAF, 0)
    tree.root = tree.new(rootNode)

    // Insert some keys
    rootNode = tree.get(tree.root)
    rootNode = treeInsert(tree, rootNode, []byte("apple"), []byte("fruit"))
    rootNode = treeInsert(tree, rootNode, []byte("banana"), []byte("fruit"))
    rootNode = treeInsert(tree, rootNode, []byte("carrot"), []byte("vegetable"))

    fmt.Println("Inserted keys")

    // Print keys
    for i := uint16(0); i < rootNode.nkeys(); i++ {
        fmt.Printf("Key: %s, Val: %s\n", rootNode.getKey(i), rootNode.getVal(i))
    }

	// fmt.Println("B-Tree structure:")
	// rootNode := tree.get(tree.root)
	// printNode(tree, rootNode, 0)
}