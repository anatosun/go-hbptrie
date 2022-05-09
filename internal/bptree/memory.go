package bptree

import "hbtrie/internal/pool"

func (bpt *BPlusTree) where(id uint64) (*pool.Node, error) {

	return bpt.list.Query(id), nil

}

func (bpt *BPlusTree) allocate() (uint64, error) {

	node, err := bpt.list.NewNode()
	return node.Id, err
}
