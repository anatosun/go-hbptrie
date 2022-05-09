package bptree

import "hbtrie/internal/pool"

func (bpt *BPlusTree) where(id uint64) (*pool.Node, error) {

	return bpt.frame.Query(id), nil

}

func (bpt *BPlusTree) allocate() (uint64, error) {

	node, err := bpt.frame.NewNode()
	return node.Id, err
}
