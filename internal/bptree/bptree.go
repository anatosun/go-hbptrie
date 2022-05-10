package bptree

import (
	"hbtrie/internal/kverrors"
	"hbtrie/internal/pool"
)

type BPlusTree struct {
	order   uint64 // number of Entries per leaf
	fanout  uint64 // number of children per internal node
	pool    *pool.Bufferpool
	frameId uint64
	root    *pool.Node
	size    int
}

func NewBplusTree(pool *pool.Bufferpool) *BPlusTree {

	bpt := &BPlusTree{}
	bpt.pool = pool
	bpt.frameId = pool.Register()
	root, err := bpt.allocate()
	if err != nil {
		panic(err)
	}
	bpt.root, err = bpt.where(root)
	if err != nil {
		panic(err)
	}

	bpt.order = 75
	bpt.fanout = 75

	return bpt
}

// Insert puts a key/value pair in the B+ tree.
func (bpt *BPlusTree) Insert(key [16]byte, value [8]byte) (success bool, err error) {

	e := pool.Entry{Key: key, Value: value}

	success, err = bpt.insert(e)

	if success {
		bpt.size++
		return success, nil
	}

	return success, err
}

// Remove deletes a given key and its entry in the B+ tree.
// This deletion is lazy, it only deletes the entry in the node without rebaleasing the tree.
func (bpt *BPlusTree) Remove(key [16]byte) (value *[8]byte, err error) {

	if id, at, found, err := bpt.search(bpt.root.Id, key); err != nil {
		return nil, err
	} else if found {
		node, err := bpt.where(id)

		if err != nil {
			return nil, err
		}

		e, err := node.DeleteEntryAt(at)

		if err != nil {
			return nil, err
		}
		bpt.size--

		return &e.Value, err
	}

	return nil, &kverrors.KeyNotFoundError{Value: key}

}

// Search returns the valu for a given key among the nodes of the B+tree.
// If the key is not found, it returns a nil pointer and an error.
func (bpt *BPlusTree) Search(key [16]byte) (*[8]byte, error) {

	if id, at, found, err := bpt.search(bpt.root.Id, key); err != nil {
		return nil, err
	} else if found {
		n, err := bpt.where(id)
		if err != nil {
			return nil, err
		}
		return &n.Entries[at].Value, err
	}

	return nil, &kverrors.KeyNotFoundError{Value: key}

}

// Len returns the length of the B+ tree
func (bpt *BPlusTree) Len() int { return bpt.size }

// search recursively search for a key in the node and its children.
func (bpt *BPlusTree) search(id uint64, key [16]byte) (child uint64, at int, found bool, err error) {

	node, err := bpt.where(id)
	if err != nil {
		return 0, 0, false, err
	}

	at, found = node.Search(key)

	if node.IsLeaf() {
		return id, at, found, nil
	}

	if found {
		at++
	}
	childID := node.Children[at]

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

	if n.IsLeaf() {
		bpt.splitLeaf(p, n, sibling, i)
	} else {
		bpt.splitNode(p, n, sibling, i)
	}

	return nil
}

// split the (internal) node into the given three nodes
func (bpt *BPlusTree) splitNode(left, middle, right *pool.Node, i int) error {
	parentKey := middle.Entries[bpt.fanout-1]
	copy(right.Entries[:], middle.Entries[:bpt.fanout])
	right.NumberOfEntries = bpt.fanout - 1
	copy(middle.Entries[:], middle.Entries[bpt.fanout:])
	middle.NumberOfEntries = bpt.fanout
	copy(right.Children[:], middle.Children[:bpt.fanout])
	right.NumberOfChildren = bpt.fanout
	copy(middle.Children[:], middle.Children[bpt.fanout:])
	middle.NumberOfChildren = bpt.fanout
	err := left.InsertChildAt(i, right)
	if err != nil {
		return err
	}
	err = left.InsertEntryAt(i, parentKey)
	if err != nil {
		return err
	}
	return nil
}

// split the leaf into the given three nodes
func (bpt *BPlusTree) splitLeaf(left, middle, right *pool.Node, i int) error {
	right.Next = middle.Next
	right.Prev = middle.Id
	middle.Next = right.Id

	copy(right.Entries[:], middle.Entries[bpt.order:])
	right.NumberOfEntries = bpt.order - 1
	copy(middle.Entries[:], middle.Entries[:bpt.order])
	middle.NumberOfEntries = bpt.order
	err := left.InsertChildAt(i+1, right)
	if err != nil {
		return err
	}
	err = left.InsertEntryAt(i, right.Entries[0])
	if err != nil {
		return err
	}
	return nil

}
