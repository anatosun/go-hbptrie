package pool

import (
	"encoding/binary"
	"fmt"
	"hbtrie/internal/kverrors"
	"os"
	"sort"
)

// number of maximum frames per pool
const limit = 100000

type Bufferpool struct {
	frames     map[uint64]*frame
	allocation uint64
	file       *os.File
}

func (pool *Bufferpool) metaHeaderSize() uint64 {
	return 8 + metaSize()*limit
}

func (pool *Bufferpool) pagePosition(frameId, pageId uint64) uint64 {
	return pool.metaHeaderSize() + pageId*uint64(PageSize)
}
func (pool *Bufferpool) metaPosition(frameId uint64) uint64 {
	return frameId * metaSize()
}

// NewBufferpool returns a new bufferpool with the given underlying file and allocation size.
// The read/write to disk will be performed from/to the given file.
// The allocation size is the number of pages that will be allocated for each frame before IO operations.
func NewBufferpool(file *os.File, allocation uint64) *Bufferpool {
	return &Bufferpool{file: file, frames: make(map[uint64]*frame), allocation: allocation}
}

func (pool *Bufferpool) write(frameId uint64, page *Node) error {
	position := pool.pagePosition(frameId, page.Id)
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
	if frameId == 0 {
		return nil, &kverrors.InvalidFrameIdError{}
	}
	position := pool.pagePosition(frameId, pageId)
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
func (pool *Bufferpool) Register() (uint64, error) {

	r := uint64(1)
	for pool.frames[r] != nil {
		r++
		if r == limit {
			return 0, &kverrors.BufferPoolLimitError{}
		}
	}
	pool.frames[r] = newFrame(pool.allocation)
	return r, nil
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
func (pool *Bufferpool) SetRoot(frameId uint64, pageId uint64) error {

	frame := pool.frames[frameId]
	if frame == nil {
		return &kverrors.UnregisteredError{}
	}

	frame.setRoot(pageId)

	return nil

}

// Returns the pageId of the b+ tree in a given frameId
func (pool *Bufferpool) GetRoot(frameId uint64) (uint64, error) {

	frame := pool.frames[frameId]
	if frame == nil {
		return 0, &kverrors.UnregisteredError{}
	}

	return frame.getRoot(), nil

}

func (pool *Bufferpool) Update(frameId, root, size uint64) error {

	frame := pool.frames[frameId]
	if frame == nil {
		return &kverrors.UnregisteredError{}
	}

	frame.update(root, size)

	return nil

}

func (pool *Bufferpool) readMetadata(frameID uint64) (metadata, error) {
	meta := metadata{0, 0, 0}

	position := pool.metaPosition(frameID)
	data := make([]byte, metaSize())
	nbytes, err := pool.file.ReadAt(data, int64(position))
	if err != nil {
		return meta, err
	}
	if nbytes != len(data) {
		return meta, &kverrors.PartialReadError{Total: len(data), Read: nbytes}
	}
	err = meta.UnmarshalBinary(data)
	if err != nil {
		return meta, err
	}
	if meta.root == 0 {
		return meta, &kverrors.InvalidMetadataError{Root: meta.root, Size: meta.size}
	}
	return meta, nil

}

func (pool *Bufferpool) writeMetadata(frameID uint64, meta metadata) error {
	position := pool.metaPosition(frameID)
	data, err := meta.MarshalBinary()
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

func (pool *Bufferpool) getFrameIds() []uint64 {
	keys := make([]uint64, 0, len(pool.frames))

	for k := range pool.frames {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	return keys
}

func (pool *Bufferpool) WriteTrie(rootFrame uint64) error {
	frameIds := pool.getFrameIds()
	nframes := len(frameIds)
	if nframes == 0 {
		return nil
	}
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, uint64(nframes))
	nbytes, err := pool.file.WriteAt(data, 0)
	if err != nil {
		return err
	}
	if nbytes != len(data) {
		return &kverrors.PartialWriteError{Total: len(data), Written: nbytes}
	}

	for _, k := range frameIds {
		err := pool.WriteTree(k)
		if err != nil {
			return err
		}
	}

	return nil
}

func (pool *Bufferpool) ReadTrie() (root uint64, size uint64, err error) {
	data := make([]byte, 8)
	nbytes, err := pool.file.ReadAt(data, 0)
	if err != nil {
		return 0, 0, err
	}
	if nbytes != len(data) {
		return 0, 0, &kverrors.PartialReadError{Total: len(data), Read: nbytes}
	}
	nframes := binary.LittleEndian.Uint64(data)

	for id := uint64(1); id <= nframes; id++ {
		if id == 1 {
			root, size, err = pool.ReadTree(id)
			if err != nil || root == 0 {
				return 0, 0, err
			}
			continue
		}
		r, _, err := pool.ReadTree(id)
		if err != nil {
			return 0, 0, err
		}
		if r == 0 {
			return root, size, nil
		}

	}

	return root, size, nil
}

func (pool *Bufferpool) WriteTree(frameId uint64) error {

	frame := pool.frames[frameId]
	if frame == nil {
		return &kverrors.UnregisteredError{}
	}

	err := pool.writeMetadata(frameId, metadata{root: frame.root, size: frame.size, cursor: frame.cursor})
	if err != nil {
		return err
	}

	for _, node := range frame.pages {
		if node.Dirty {
			err := pool.write(frameId, node)
			if err != nil {
				return err
			}
		}
	}

	return nil

}

func (pool *Bufferpool) ReadTree(frameId uint64) (uint64, uint64, error) {
	if frameId > limit {
		return 0, 0, &kverrors.InvalidFrameIdError{}
	}

	meta, err := pool.readMetadata(frameId)
	if err != nil {
		fmt.Println("error on frameid", frameId, err)
		return 0, 0, err
	}
	rootId := meta.root
	size := meta.size
	root, err := pool.io(frameId, rootId)
	if err != nil {
		fmt.Println("error on root", rootId, err)

		return 0, 0, err
	}
	if root.Page == nil {
		fmt.Println("error on root", rootId, err)

		return 0, 0, &kverrors.InvalidNodeError{}
	}

	frame := newFrame(pool.allocation)
	frame.root = rootId
	frame.size = size
	frame.cursor = meta.cursor
	pool.frames[frameId] = frame

	return rootId, size, nil

}
