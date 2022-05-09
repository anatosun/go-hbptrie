package pool

var PageSize uint16 = 4096

// Page is the unit of the Bufferpool
type Page struct {
	Id    uint64 // 8 byte
	prev  *Page  // 8 byte
	next  *Page  // 8 byte
	Dirty bool   // 1 byte
}

func NewPage(id uint64) *Page {
	return &Page{Id: id, Dirty: true, prev: nil, next: nil}
}
