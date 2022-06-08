package pool

import (
	"errors"
	"fmt"
	"hbtrie/internal/kverrors"
	"os"
	"path/filepath"
	"sort"
)

// number of maximum frames per pool
const poolMaxNumberOfTrees = 100000
const hbFilename = "hb_meta.dbm"

type Bufferpool struct {
	frames     map[uint64]*frame
	allocation uint64
	dataPath   string
	file       *os.File
}

// NewBufferpool returns a new bufferpool with the given underlying file and allocation size.
// The read/write to disk will be performed from/to the given file.
// The allocation size is the number of pages that will be allocated for each frame before IO operations.
func NewBufferpool(allocation uint64, dataPath string) (*Bufferpool, error) {
	dp := filepath.Join(dataPath, "hbdata/")
	err := os.MkdirAll(dp, 0755)
	if err != nil {
		return nil, err
	}
	hbf := filepath.Join(dp, hbFilename)
	var file *os.File
	_, err = os.Stat(hbf)
	if errors.Is(err, os.ErrNotExist) {
		file, err = os.Create(hbf)
		if err != nil {
			return nil, err
		}
	} else {
		file, err = os.OpenFile(hbf, os.O_RDWR, 0755)
		if err != nil {
			return nil, err
		}
	}
	pool := &Bufferpool{frames: make(map[uint64]*frame), allocation: allocation, dataPath: dp, file: file}

	return pool, err
}

func (pool *Bufferpool) write(frameId uint64, page *Node) error {
	frame := pool.frames[frameId]
	if frame == nil {
		return &kverrors.UnregisteredError{}
	}
	file := frame.file
	position := pagePosition(page.Id)
	data, err := page.MarshalBinary()
	if err != nil {
		return err
	}
	nbytes, err := file.WriteAt(data, int64(position))
	if err != nil {
		return err
	}
	if nbytes != len(data) {
		return &kverrors.PartialWriteError{Total: len(data), Written: nbytes}
	}
	return nil

}

func (pool *Bufferpool) io(frameId, pageId uint64) (*Node, error) {
	filename := pool.filename(frameId)
	_, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(filename, os.O_RDWR, 0755)
	if err != nil {
		return nil, err
	}
	if frameId == 0 {
		return nil, &kverrors.InvalidFrameIdError{}
	}
	position := pagePosition(pageId)
	data := make([]byte, PageSize)
	nbytes, err := file.ReadAt(data, int64(position))
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

func (pool *Bufferpool) filename(frameId uint64) string {
	filename := fmt.Sprintf("frame_%d.db", frameId)
	filename = filepath.Join(pool.dataPath, filename)
	return filename
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

	filename := pool.filename(r)
	file, err := os.Create(filename)
	if err != nil {
		return 0, err
	}
	pool.frames[r] = newFrame(file, pool.allocation)
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

func (pool *Bufferpool) readMetadata(frameID uint64) (frameMetadata, error) {
	meta := frameMetadata{0, 0, 0}
	filename := pool.filename(frameID)
	_, err := os.Stat(filename)
	if err != nil {
		return meta, err
	}
	file, err := os.OpenFile(filename, os.O_RDWR, 0755)
	if err != nil {
		return meta, err
	}
	position := uint64(0)
	data := make([]byte, frameMetaSize())
	nbytes, err := file.ReadAt(data, int64(position))
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

func (pool *Bufferpool) writeMetadata(frameId uint64, meta frameMetadata) error {
	frame := pool.frames[frameId]
	if frame == nil {
		return &kverrors.UnregisteredError{}
	}
	file := frame.file
	position := int64(0)
	data, err := meta.MarshalBinary()
	if err != nil {
		return err
	}
	nbytes, err := file.WriteAt(data, position)
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

	err := pool.writeMetadata(frameId, frameMetadata{root: frame.root, size: frame.size, cursor: frame.cursor})
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
	filename := pool.filename(frameId)
	_, err = os.Stat(filename)
	if err != nil {
		return 0, 0, err
	}
	file, err := os.OpenFile(filename, os.O_RDWR, 0755)
	if err != nil {
		return 0, 0, err
	}
	frame := newFrame(file, pool.allocation)
	frame.root = meta.root
	frame.size = meta.size
	frame.cursor = meta.cursor
	pool.frames[frameId] = frame

	return meta.root, meta.size, nil

}

func (pool *Bufferpool) Close() error {
	for _, frame := range pool.frames {
		if frame != nil {
			err := frame.file.Close()
			if err != nil {
				return err
			}
		}
	}
	return pool.file.Close()
}

func (pool *Bufferpool) Clean() error {
	return os.RemoveAll(pool.dataPath)
}

func (pool *Bufferpool) WriteTrie(root, size uint64) error {
	frameIds := pool.getFrameIds()
	nframes := uint64(len(frameIds))
	meta := &hbMetatadata{root: root, size: size, nframes: nframes}
	file := pool.file
	position := int64(0)
	data, err := meta.MarshalBinary()
	if err != nil {
		return err
	}
	nbytes, err := file.WriteAt(data, position)
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

func (pool *Bufferpool) ReadTrie() (root uint64, size uint64, nframes uint64, err error) {
	meta := hbMetatadata{}
	file := pool.file
	position := int64(0)
	data := make([]byte, hbMetaSize())
	nbytes, err := file.ReadAt(data, position)
	if err != nil {
		fmt.Println(err)

		return 0, 0, 0, err
	}
	if nbytes != len(data) {
		return 0, 0, 0, &kverrors.PartialReadError{Total: len(data), Read: nbytes}
	}
	err = meta.UnmarshalBinary(data)
	if err != nil {
		return 0, 0, 0, err
	}

	if meta.root == 0 {
		return 0, 0, 0, &kverrors.InvalidMetadataError{Root: meta.root, Size: meta.size}
	}

	root, size, nframes = meta.root, meta.size, meta.nframes

	for id := uint64(1); id < nframes+1; id++ {
		if id == 1 {
			rootPageId, _, err := pool.ReadTree(id)
			if err != nil {
				return 0, 0, nframes, err
			}
			if rootPageId == 0 {
				return 0, 0, nframes, &kverrors.InvalidNodeError{}
			}
			continue
		}
		r, _, err := pool.ReadTree(id)
		if err != nil {
			return 0, 0, 0, err
		}
		if r == 0 {
			return root, size, nframes, &kverrors.InvalidNodeError{}
		}

	}

	return root, size, nframes, nil
}
