package bptree

import (
	"hbtrie/internal/operations"
	"hbtrie/internal/pool"
)

func (bpt *BPlusTree) insert(e pool.Entry) (bool, error) {
	if bpt.full(bpt.root) {

		id1, errAlloc1 := bpt.allocate()
		id2, errAlloc2 := bpt.allocate()

		if errAlloc1 != nil {
			return false, errAlloc1
		}
		if errAlloc2 != nil {
			return false, errAlloc2
		}

		n1, errFetching1 := bpt.where(id1)
		n2, errFetching2 := bpt.where(id2)

		if errFetching1 != nil {
			return false, errFetching1
		}
		if errFetching2 != nil {
			return false, errFetching2
		}

		newRoot := n1
		rightSibling := n2
		oldRoot := bpt.root

		newRoot.InsertChildAt(0, oldRoot)
		bpt.root = newRoot

		if err := bpt.split(newRoot.Id, oldRoot.Id, rightSibling.Id, 0); err != nil {
			return false, err
		}

	}

	return bpt.path(bpt.root.Id, e)
}

func (bpt *BPlusTree) path(id uint64, e pool.Entry) (bool, error) {

	node, err := bpt.where(id)
	if err != nil {
		return false, err
	}

	if node.IsLeaf() {
		return bpt.insertLeaf(id, e)
	}

	return bpt.insertInternal(id, e)
}

func (bpt *BPlusTree) insertLeaf(id uint64, e pool.Entry) (bool, error) {

	n, err := bpt.where(id)
	if err != nil {
		return false, err
	}

	at, found := n.Search(e.Key)

	if found {
		err := n.Update(at, e.Value)
		if err != nil {
			return false, err
		}
		return false, err
	}

	err = n.InsertEntryAt(at, e)
	if err != nil {
		return false, err
	}

	return true, err
}

func (bpt *BPlusTree) insertInternal(id uint64, e pool.Entry) (bool, error) {

	node, err := bpt.where(id)
	if err != nil {
		return false, err
	}

	at, found := node.Search(e.Key)
	if found {
		at++
	}

	childID := node.Children[at]

	child, err := bpt.where(childID)

	if err != nil {

		return false, err
	}

	if bpt.full(child) {

		newid, err := bpt.allocate()
		if err != nil {
			return false, err
		}

		sibling, err := bpt.where(newid)

		if err != nil {

			return false, err
		}

		if err := bpt.split(node.Id, child.Id, sibling.Id, at); err != nil {

			return false, err
		}

		if operations.Compare(e.Key, node.Entries[at].Key) >= 0 {

			newChildID := node.Children[at+1]
			child, err = bpt.where(newChildID)

			if err != nil {

				return false, err
			}

		}
	}
	return bpt.path(child.Id, e)
}

func (bpt *BPlusTree) full(n *pool.Node) bool {
	if n.IsLeaf() {
		return n.NumberOfEntries == (2*bpt.fanout)-1
	}
	return n.NumberOfEntries == (2*bpt.order)-1
}
