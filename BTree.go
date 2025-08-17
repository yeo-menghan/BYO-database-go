/* Set Up */

type BNode struct {
	data []byte // content stored as raw bytes, can be dumped to the disk
}

const (
	BNode_Node = 1 // internal nodes without values, contain key & values
	BNode_Leaf = 2 // leaf nodes with values
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
	asset(idx < node.nkeys())
	pos := HEADER + 8*idx
	return binary.LittleEndian.Uint64(node.data[pos:])
}
func (node BNode) setPtr(idx uint16, val uint64) {
	asset(idx < node.nkeys())
	pos := HEADER + 8*idx
	binary.LittleEndian.PutUint64(node.data[pos:], val)
}

// offset list
// allow direct addressing of any KV pair without scanning all previous pairs; provide indexing pointers
// each offset corresponds to a key-value pair index (starting from 1)
// offset starts after pointers array (4 + nkeys*8), 2 bytes each, idx is 1-based
func offsetPos(node BNode, idx uint16) uint16 {
	asset(1 <= idx && idx <= node.nkeys())
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
	asset(idx <= node.nkeys())
	return HEADER + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}
func (node BNode) getKey(idx uint16) []byte {
	asset(idx < node.nkeys())
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node.data[pos:])
	return node.data[pos+4:][:klen]
}
func (node BNode) getVal(idx uint16) []byte {
	asset(idx < node.nkeys())
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
