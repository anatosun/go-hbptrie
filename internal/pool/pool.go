package pool

import (
	"hbtrie/internal/kverrors"
	"os"
)

type Bufferpool struct {
	frames     map[uint64]*frame
	allocation uint64
	file       *os.File
}

func (pool *Bufferpool) position(frameId, pageId uint64) uint64 {
	return frameId*pool.allocation + pageId*uint64(PageSize)
}

// NewBufferpool returns a new bufferpool with the given underlying file and allocation size.
// The read/write to disk will be performed from/to the given file.
// The allocation size is the number of pages that will be allocated for each frame before IO operations.
func NewBufferpool(file *os.File, allocation uint64) *Bufferpool {
	return &Bufferpool{file: file, frames: make(map[uint64]*frame), allocation: allocation}
}

func (pool *Bufferpool) write(frameId uint64, page *Node) error {
	position := pool.position(frameId, page.Id)
	data, err := page.MarshalBinary()
	if err != nil {
		return err
	}
	nbytes, err := pool.file.WriteAt(data, int64(position))
	if err != nil {
		return err
	}
	if nbytes != len(data) {
		return &kverrors.PartialWriteError{Total: len(data), Written: nbytes}
	}
	return nil

}

func (pool *Bufferpool) io(frameId, pageId uint64) (*Node, error) {

	position := pool.position(frameId, pageId)
	data := make([]byte, PageSize)
	nbytes, err := pool.file.ReadAt(data, int64(position))
	if err != nil {
		return nil, err
	}
	if nbytes != len(data) {
		return nil, &kverrors.PartialReadError{Total: len(data), Read: nbytes}
	}
	node := &Node{Page: NewPage(0)}
	err = node.UnmarshalBinary(data)
	if err != nil {
		return nil, err
	}
	if node.Page.Id == 0 {
		return nil, &kverrors.InvalidNodeError{}
	}
	return node, nil
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

// Query returns the node with the given id from the given frame. If the node is not memory, it performs IO.
// It may return an error if the client hasn't previously registered the frame (i.e., the frame id is invalid).
func (pool *Bufferpool) Query(frameId, pageID uint64) (node *Node, err error) {

	frame := pool.frames[frameId]
	if frame == nil {
		return nil, &kverrors.UnregisteredError{}
	}

	node = frame.query(pageID)
	if node == nil {
		node, err = pool.io(frameId, pageID)
		if err != nil {
			return nil, err
		}
		for frame.full() {
			tail := frame.evict()
			pool.write(frameId, tail)
		}
		err = frame.add(node)
		if err != nil {
			return nil, err
		}
	}

	return node, nil

}

// NewPage is an alias to NewNode
func (pool *Bufferpool) NewPage(frameId uint64) (*Node, error) {
	return pool.NewNode(frameId)
}

// NewNode provides a new node (page) in the frame given as parameter.
// It may return an error if the client hasn't previously registered the frame (i.e., the frame id is invalid).
func (pool *Bufferpool) NewNode(frameId uint64) (*Node, error) {

	frame := pool.frames[frameId]
	if frame == nil {
		return nil, &kverrors.UnregisteredError{}
	}

	node, full := frame.newNode()
	for full {
		tail := frame.evict()
		if tail != nil && tail.Dirty {
			err := pool.write(frameId, tail)
			if err != nil {
				return nil, err
			}
		}
		node, full = frame.newNode()
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
