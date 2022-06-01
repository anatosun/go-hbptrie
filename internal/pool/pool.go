package pool

import (
	"encoding/binary"
	"hbtrie/internal/kverrors"
	"os"
	"sort"
)

// number of maximum frames per pool
const poolMaxNumberOfTrees = 10000

type Bufferpool struct {
	frames     map[uint64]*frame
	allocation uint64
	file       *os.File
}

func (pool *Bufferpool) pagePosition(frameId, pageId uint64) uint64 {
	return pool.metaMaxHeaderSize() + frameId*pool.treeMaxSize() + pageId*PageSize
}
func (pool *Bufferpool) treeMaxSize() uint64 {
	return PageSize * frameMaxNumberOfPages
}
func (pool *Bufferpool) metaPosition(frameId uint64) uint64 {
	return 8 + 8 + frameId*metaSize()
}
func (pool *Bufferpool) metaMaxHeaderSize() uint64 {
	return pool.metaPosition(poolMaxNumberOfTrees)
}

// NewBufferpool returns a new bufferpool with the given underlying file and allocation size.
// The read/write to disk will be performed from/to the given file.
// The allocation size is the number of pages that will be allocated for each frame before IO operations.
func NewBufferpool(file *os.File, allocation uint64) *Bufferpool {

	pool := &Bufferpool{file: file, frames: make(map[uint64]*frame), allocation: allocation}
	// size := int64(pool.pagePosition(poolMaxNumberOfTrees, frameMaxNumberOfPages))
	// file.Truncate(size)
	return pool
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
	if pool.file == nil {
		return nil, &kverrors.UnspecifiedFileError{}
	}
	if frameId == 0 {
		return nil, &kverrors.InvalidFrameIdError{}
	}
	position := pool.pagePosition(frameId, pageId)
	from := pool.pagePosition(frameId, frameMaxNumberOfPages)
	to := pool.pagePosition(frameId+1, 1) - 1
	if position > to {
		return nil, &kverrors.OutsideOfRangeError{From: from, To: to, Actual: position}
	}
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

// Register is used for a client to get a frame allocated in the bufferpool.
// It returns the id of the frame which should be use for subsequent queries.
func (pool *Bufferpool) Register() (uint64, error) {

	r := uint64(1)
	for pool.frames[r] != nil {
		r++
		if r == poolMaxNumberOfTrees {
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

func (pool *Bufferpool) GetFrames() []uint64 {
	return pool.getFrameIds()
}

// GetNodes returns the map of nodes in the given frame.
func (pool *Bufferpool) GetNodes(frameId uint64) (map[uint64]*Node, error) {
	frame := pool.frames[frameId]
	if frame == nil {
		return nil, &kverrors.UnregisteredError{}
	}

	return frame.pages, nil
}

// Query returns the node with the given id from the given frame. If the node is not memory, it performs IO.
// It may return an error if the client hasn't previously registered the frame (i.e., the frame id is invalid).
func (pool *Bufferpool) Query(frameId, pageID uint64) (node *Node, err error) {

	frame := pool.frames[frameId]
	if frame == nil {
		return nil, &kverrors.UnregisteredError{}
	}

	node = frame.query(pageID)
	for node == nil {
		node, err = pool.io(frameId, pageID)
		if err != nil {
			// log.Default().Printf("Query: %d %d: %v", frameId, pageID, err)
			return nil, err
		}
		for frame.full() {
			tail := frame.evict()
			pool.write(frameId, tail)
		}
		err = frame.add(node)
		if err != nil {
			// log.Default().Printf("Query: %d %d: %v", frameId, pageID, err)
			return nil, err
		}
	}
	// log.Default().Printf("Query: %d %d: success", frameId, pageID)

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

// Update allows to update the root/size information of the b+ tree in a given frameId.
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

func (pool *Bufferpool) WriteTree(frameId uint64) error {

	frame := pool.frames[frameId]
	if frame == nil {
		return &kverrors.UnregisteredError{}
	}

	err := pool.writeMetadata(frameId, metadata{root: frame.root, size: frame.size, cursor: frame.cursor})
	if err != nil {
		return err
	}
	max := uint64(0)
	for _, node := range frame.pages {
		if node.Dirty {
			err := pool.write(frameId, node)
			if err != nil {
				return err
			}
		}
		if node.Id > max {
			max = node.Id
		}
	}

	return nil

}

func (pool *Bufferpool) ReadTree(frameId uint64) (uint64, uint64, error) {
	if frameId > poolMaxNumberOfTrees {
		return 0, 0, &kverrors.InvalidFrameIdError{}
	}

	meta, err := pool.readMetadata(frameId)
	if err != nil {
		return 0, 0, err
	}
	root, err := pool.io(frameId, meta.root)
	if err != nil {

		return 0, 0, err
	}
	if root.Page == nil {

		return 0, 0, &kverrors.InvalidNodeError{}
	}
	if root.Page.Id == 0 {
		return 0, 0, &kverrors.InvalidNodeError{}
	}

	frame := newFrame(pool.allocation)
	frame.root = meta.root
	frame.size = meta.size
	frame.cursor = meta.cursor
	pool.frames[frameId] = frame

	return meta.root, meta.size, nil

}

func (pool *Bufferpool) WriteTrie(size uint64) error {
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

	data = make([]byte, 8)
	binary.LittleEndian.PutUint64(data, uint64(size))
	nbytes, err = pool.file.WriteAt(data, 8)
	if err != nil {
		return err
	}
	if nbytes != len(data) {
		return &kverrors.PartialWriteError{Total: len(data), Written: nbytes}
	}

	for _, frameId := range frameIds {
		err := pool.WriteTree(frameId)
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
	data = make([]byte, 8)
	nbytes, err = pool.file.ReadAt(data, 8)
	if err != nil {
		return 0, 0, err
	}
	if nbytes != len(data) {
		return 0, 0, &kverrors.PartialReadError{Total: len(data), Read: nbytes}
	}
	size = binary.LittleEndian.Uint64(data)
	for id := uint64(1); id < nframes+1; id++ {
		if id == 1 {
			root, _, err = pool.ReadTree(id)
			if err != nil {
				return 0, 0, err
			}
			if root == 0 {
				return 0, 0, &kverrors.InvalidNodeError{}
			}
			continue
		}
		r, _, err := pool.ReadTree(id)
		if err != nil {
			return 0, 0, err
		}
		if r == 0 {
			return root, size, &kverrors.InvalidNodeError{}
		}
		// fmt.Printf("%d %d ", r, s)

	}

	// fmt.Printf("\n")

	return root, size, nil
}
