package bptree

import "hbtrie/internal/pool"

func (bpt *BPlusTree) where(id uint64) (*pool.Node, error) {

	return bpt.pool.Query(bpt.frameId, id)

}

func (bpt *BPlusTree) allocate() (uint64, error) {

	node, err := bpt.pool.NewNode(bpt.frameId)
	return node.Id, err
}
