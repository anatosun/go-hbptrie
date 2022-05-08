package bptree

import (
	"hbtrie/internal/pool"
)

type BPlusTree struct {
	order  uint64 // number of entries per leaf
	fanout uint64 // number of children per internal node
	list   *pool.List
	nodes  map[uint64]*node
	root   *node
	size   int
}

func NewBplusTree() *BPlusTree {

	bpt := &BPlusTree{}
	bpt.nodes = make(map[uint64]*node)
	bpt.list = pool.NewList()
	root, err := bpt.allocate()
	if err != nil {
		panic(err)
	}
	bpt.root, err = bpt.where(root)
	if err != nil {
		panic(err)
	}
	bpt.nodes[bpt.root.Id] = bpt.root

	bpt.order = 80
	bpt.fanout = 80

	return bpt
}

// serves to put a key/value pair in the B+ tree
func (bpt *BPlusTree) Insert(key [16]byte, value [8]byte) (success bool, err error) {

	e := entry{key: key, value: value}

	success, err = bpt.insert(e)

	if success {
		bpt.size++
		return success, nil
	}

	return success, err
}

// removes a given key and its entry in the B+ tree
// this deletion is lazy, it only deletes the entry in the node without rebaleasing the tree
func (bpt *BPlusTree) Remove(key [16]byte) (value *[8]byte, err error) {

	if id, at, found, err := bpt.search(bpt.root.Id, key); err != nil {
		return nil, err
	} else if found {
		node, err := bpt.where(id)

		if err != nil {
			return nil, err
		}

		e, err := node.deleteEntryAt(at)

		if err != nil {
			return nil, err
		}
		bpt.size--

		return &e.value, err
	}

	return nil, &KeyNotFoundError{Value: key}

}

// search for a given key among the nodes of the B+tree
func (bpt *BPlusTree) Search(key [16]byte) (*[8]byte, error) {

	if id, at, found, err := bpt.search(bpt.root.Id, key); err != nil {
		return nil, err
	} else if found {
		n, err := bpt.where(id)
		if err != nil {
			return nil, err
		}
		return &n.entries[at].value, err
	}

	return nil, &KeyNotFoundError{Value: key}

}

// returns the length of the B+ tree
func (bpt *BPlusTree) Len() int { return bpt.size }

// recursively search for a key in the node and its children
func (bpt *BPlusTree) search(id uint64, key [16]byte) (child uint64, at int, found bool, err error) {

	node, err := bpt.where(id)
	if err != nil {
		return 0, 0, false, err
	}

	at, found = node.search(key)

	if node.isLeaf() {
		return id, at, found, nil
	}

	if found {
		at++
	}
	childID := node.children[at]

	return bpt.search(childID, key)
}

// split the given three nodes
func (bpt *BPlusTree) split(pID, nID, siblingID uint64, i int) error {

	p, err := bpt.where(pID)
	if err != nil {
		return err
	}

	n, err := bpt.where(nID)
	if err != nil {
		return err
	}

	sibling, err := bpt.where(siblingID)
	if err != nil {
		return err
	}

	if n.isLeaf() {
		bpt.splitLeaf(p, n, sibling, i)
	} else {
		bpt.splitNode(p, n, sibling, i)
	}

	return nil
}

// split the (internal) node into the given three nodes
func (bpt *BPlusTree) splitNode(left, middle, right *node, i int) error {
	parentKey := middle.entries[bpt.fanout-1]
	copy(right.entries[:], middle.entries[:bpt.fanout])
	right.numberOfEntries = int(bpt.fanout - 1)
	copy(middle.entries[:], middle.entries[bpt.fanout:])
	middle.numberOfEntries = int(bpt.fanout)
	copy(right.children[:], middle.children[:bpt.fanout])
	right.numberOfChildren = int(bpt.fanout)
	copy(middle.children[:], middle.children[bpt.fanout:])
	middle.numberOfChildren = int(bpt.fanout)
	err := left.insertChildAt(i, right)
	if err != nil {
		return err
	}
	err = left.insertEntryAt(i, parentKey)
	if err != nil {
		return err
	}
	return nil
}

// split the leaf into the given three nodes
func (bpt *BPlusTree) splitLeaf(left, middle, right *node, i int) error {
	right.next = middle.next
	right.prev = middle.Id
	middle.next = right.Id

	copy(right.entries[:], middle.entries[bpt.order:])
	right.numberOfEntries = int(bpt.order - 1)
	copy(middle.entries[:], middle.entries[:bpt.order])
	middle.numberOfEntries = int(bpt.order)
	err := left.insertChildAt(i+1, right)
	if err != nil {
		return err
	}
	err = left.insertEntryAt(i, right.entries[0])
	if err != nil {
		return err
	}
	return nil

}
