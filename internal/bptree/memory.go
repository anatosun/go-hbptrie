package bptree

import (
	"hbtrie/internal/kverrors"
	"hbtrie/internal/pool"
)

func (bpt *BPlusTree) where(id uint64) (*pool.Node, error) {

	return bpt.pool.Query(bpt.frameId, id)

}

func (bpt *BPlusTree) allocate() (uint64, error) {

	node, err := bpt.pool.NewNode(bpt.frameId)
	if node == nil || node.Id == 0 {
		panic(&kverrors.InvalidNodeError{})
	}
	return node.Id, err
}
