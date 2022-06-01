package bptree

import (
	"hbtrie/internal/pool"
)

func (bpt *BPlusTree) where(id uint64) (*pool.Node, error) {

	return bpt.pool.Query(bpt.frameId, id)

}

func (bpt *BPlusTree) allocate() (uint64, error) {

	node, err := bpt.pool.NewNode(bpt.frameId)

	return node.Id, err
}

func MaxNumberOfPages() uint64 {
	return 100
}

// Write writes the tree to disk according to the BufferPool logic.
func (bpt *BPlusTree) Write() error {
	return bpt.pool.WriteTree(bpt.frameId)
}

// Read retrieves a B+ Tree from disk according to the BufferPool logic.
func ReadBpTreeFromDisk(pool *pool.Bufferpool, frameId uint64) (*BPlusTree, error) {
	rootId, size, err := pool.ReadTree(frameId)
	if err != nil {
		return nil, err
	}

	root, err := pool.Query(frameId, rootId)
	if err != nil {
		return nil, err
	}

	bpt := &BPlusTree{}
	bpt.pool = pool
	bpt.frameId = frameId
	bpt.size = int(size)
	bpt.root = root
	bpt.order = uint64(len(bpt.root.Entries) / 2)
	bpt.fanout = uint64(len(bpt.root.Children) / 2)
	return bpt, nil
}
