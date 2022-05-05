package pool

var pageSize uint16 = 4096

// Page is the unit of the Bufferpool
type Page struct {
	id    uint64
	prev  *Page
	next  *Page
	dirty bool
	data  []byte
}

func (p *Page) Size() uint16 {
	return pageSize
}

func NewPage() *Page {
	return &Page{dirty: true, prev: nil, next: nil}
}
