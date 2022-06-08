package pool

var PageSize uint64 = 4096

// Page is the unit of the Bufferpool

type Page struct {

	// Page Id
	Id uint64 // 8 byte

	// Dirty flag
	Dirty bool // 1 byte

	// Previous page in the frame linked list
	prev *Page // 8 byte

	// Next page in the frame linked list
	next *Page // 8 byte

}

// Initialises a new page with given id. On initialisation the dirty flag is set to true.
func NewPage(id uint64) *Page {
	return &Page{Id: id, Dirty: true, prev: nil, next: nil}
}
