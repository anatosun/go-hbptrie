package bptree

func (bpt *BPlusTree) insert(e entry) (bool, error) {
	if bpt.root.full(int(bpt.order), int(bpt.fanout)) {

		id1, errAlloc1 := bpt.allocate()
		id2, errAlloc2 := bpt.allocate()

		if errAlloc1 != nil {
			return false, errAlloc1
		}
		if errAlloc2 != nil {
			return false, errAlloc2
		}

		n1, err_fetching_1 := bpt.where(id1)
		n2, err_fetching_2 := bpt.where(id2)

		if err_fetching_1 != nil {
			return false, err_fetching_1
		}
		if err_fetching_2 != nil {
			return false, err_fetching_2
		}

		newRoot := n1
		rightSibling := n2
		oldRoot := bpt.root

		newRoot.insertChildAt(0, oldRoot)
		bpt.root = newRoot

		if err := bpt.split(newRoot.Id, oldRoot.Id, rightSibling.Id, 0); err != nil {
			return false, err
		}

	}

	return bpt.path(bpt.root.Id, e)
}

func (bpt *BPlusTree) path(id uint64, e entry) (bool, error) {

	node, err := bpt.where(id)
	if err != nil {
		return false, err
	}

	if node.isLeaf() {
		return bpt.insertLeaf(id, e)
	}

	return bpt.insertInternal(id, e)
}

func (bpt *BPlusTree) insertLeaf(id uint64, e entry) (bool, error) {

	n, err := bpt.where(id)
	if err != nil {
		return false, err
	}

	at, found := n.search(e.key)

	if found {
		err := n.update(at, e.value)
		if err != nil {
			return false, err
		}
		return false, err
	}

	err = n.insertEntryAt(at, e)
	if err != nil {
		return false, err
	}

	return true, err
}

func (bpt *BPlusTree) insertInternal(id uint64, e entry) (bool, error) {

	node, err := bpt.where(id)
	if err != nil {
		return false, err
	}

	at, found := node.search(e.key)
	if found {
		at++
	}

	childID := node.children[at]

	child, err := bpt.where(childID)

	if err != nil {

		return false, err
	}

	if child.full(int(bpt.order), int(bpt.fanout)) {

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

		if Compare(e.key, node.entries[at].key) >= 0 {

			newChildID := node.children[at+1]
			child, err = bpt.where(newChildID)

			if err != nil {

				return false, err
			}

		}
	}
	return bpt.path(child.Id, e)
}
