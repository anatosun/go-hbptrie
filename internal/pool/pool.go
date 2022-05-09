package pool

import (
	"hbtrie/internal/kverrors"
	"math/rand"
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

func NewBufferpool(file *os.File, allocation uint64) *Bufferpool {
	return &Bufferpool{file: file, frames: make(map[uint64]*frame)}
}

func (pool *Bufferpool) write(page *Page) error {
	return nil
}

func (pool *Bufferpool) read(page *Page) error {
	return nil
}

func (pool *Bufferpool) io() {

}

// Register is used for a client to get a frame allocated in the bufferpool.
// It returns the id of the frame which should be use for subsequent queries.
func (pool *Bufferpool) Register() uint64 {

	r := rand.Uint64()
	for pool.frames[r] != nil {
		r = rand.Uint64()
	}

	pool.frames[r] = newFrame(pool.allocation)
	return r
}

func (pool *Bufferpool) Unregister(id uint64) {
	delete(pool.frames, id)
}

func (pool *Bufferpool) Query(frameId, pageID uint64) (*Node, error) {

	frame := pool.frames[frameId]
	if frame == nil {
		return nil, &kverrors.UnregisteredError{}
	}
	return frame.query(pageID), nil

}

func (pool *Bufferpool) NewNode(frameId uint64) (node *Node, err error) {

	frame := pool.frames[frameId]
	if frame == nil {
		return nil, &kverrors.UnregisteredError{}
	}

	var full bool
	for node, full = frame.newNode(); full; {
		tail := frame.tail
		frame.pop(tail)
		// here we should write to disk
	}

	return node, nil

}
