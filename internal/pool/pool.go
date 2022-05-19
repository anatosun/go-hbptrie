package pool

import (
	"hbtrie/internal/kverrors"
	"os"
)

// // Pool is an interface for a buffer pool
// type Pool interface {
// 	PageSize() uint16
// 	NewPage() *Page
// 	GetPage(pageID uint16) *Page
// 	EvictPage(pageID uint16) bool
// 	EvictPages()
// 	DeletePage(pageID uint16) error
// 	UnpinPage(pageID uint16, dirty bool) error
// }

type Bufferpool struct {
	frames     map[uint64]*frame
	allocation uint64
	file       *os.File
}

// NewBufferpool returns a new bufferpool with the given underlying file and allocation size.
// The read/write to disk will be performed from/to the given file.
// The allocation size is the number of pages that will be allocated for each frame before IO operations.
func NewBufferpool(file *os.File, allocation uint64) *Bufferpool {
	return &Bufferpool{file: file, frames: make(map[uint64]*frame), allocation: allocation}
}

// Register is used for a client to get a frame allocated in the bufferpool.
// It returns the id of the frame which should be use for subsequent queries.
func (pool *Bufferpool) Register() uint64 {

	r := uint64(0)
	for pool.frames[r] != nil {
		r++
	}
	pool.frames[r] = newFrame(pool.allocation)
	return r
}

// Unregister deletes the frame with the given id. This operation is irreversible.
func (pool *Bufferpool) Unregister(id uint64) {
	delete(pool.frames, id)
}

// Query returns the node with the given id from the given frame.
// It may return an error if the client hasn't previously registered the frame (i.e., the frame id is invalid).
func (pool *Bufferpool) Query(frameId, pageID uint64) (*Node, error) {

	frame := pool.frames[frameId]
	if frame == nil {
		return nil, &kverrors.UnregisteredError{}
	}
	return frame.query(pageID), nil

}

// NewPage is an alias to NewNode
func (pool *Bufferpool) NewPage(frameId uint64) (*Node, error) {
	return pool.NewNode(frameId)
}

// NewNode provides a new node (page) in the frame given as parameter.
// It may return an error if the client hasn't previously registered the frame (i.e., the frame id is invalid).
func (pool *Bufferpool) NewNode(frameId uint64) (node *Node, err error) {

	frame := pool.frames[frameId]
	if frame == nil {
		return nil, &kverrors.UnregisteredError{}
	}

	full := false
	for node, full = frame.newNode(); full; {
		// tail := frame.evictTail()
		// here we should write to disk
	}

	return node, nil

}

// Sets the pageId of the b+ tree in a given frameId
func (pool *Bufferpool) SetRootPageId(frameId uint64, pageId uint64) error {

	frame := pool.frames[frameId]
	if frame == nil {
		return &kverrors.UnregisteredError{}
	}

	frame.setRootPageId(pageId)

	return nil

}

// Returns the pageId of the b+ tree in a given frameId
func (pool *Bufferpool) GetRootPageId(frameId uint64) (uint64, error) {

	frame := pool.frames[frameId]
	if frame == nil {
		return 0, &kverrors.UnregisteredError{}
	}

	return frame.getRootPageId(), nil

}
