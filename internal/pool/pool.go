package pool

import "os"

// Pool is an interface for a buffer pool
type Pool interface {
	PageSize() uint16
	NewPage() *Page
	GetPage(pageID uint16) *Page
	EvictPage(pageID uint16) bool
	EvictPages()
	DeletePage(pageID uint16) error
	UnpinPage(pageID uint16, dirty bool) error
}

type Bufferpool struct {
	frames []*Frame
	file   *os.File
}

func newBufferpool(file *os.File) *Bufferpool {
	return &Bufferpool{file: file, frames: nil}
}

func (pool *Bufferpool) write(page *Page) error {
	return nil
}

func (pool *Bufferpool) read(page *Page) error {
	return nil
}

func (pool *Bufferpool) io() {

}
