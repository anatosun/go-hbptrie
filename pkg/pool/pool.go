package pool

// Page is the unit of the Bufferpool
type Page struct {
}

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
