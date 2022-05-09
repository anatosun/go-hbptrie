package bptree

import "hbtrie/internal/pool"

func (bpt *BPlusTree) where(id uint64) (*pool.Node, error) {

	if node, ok := bpt.nodes[id]; ok {
		return node, nil
	}

	page := bpt.list.Query(id)
	node := page
	bpt.nodes[id] = node
	return node, nil

}

func (bpt *BPlusTree) allocate() (uint64, error) {

	page, err := bpt.list.NewNode()
	bpt.nodes[page.Id] = page
	return page.Id, err

}
