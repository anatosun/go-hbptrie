package bptree

func (bpt *BPlusTree) where(id uint64) (*node, error) {

	if node, ok := bpt.nodes[id]; ok {
		return node, nil
	}

	page := bpt.list.Query(id)
	node := &node{Page: page}
	bpt.nodes[id] = node
	return node, nil

}

func (bpt *BPlusTree) allocate() (uint64, error) {

	page, err := bpt.list.Allocate()
	bpt.nodes[page.Id] = &node{Page: page}
	return page.Id, err

}
