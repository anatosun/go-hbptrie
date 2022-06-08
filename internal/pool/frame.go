package pool

import (
	"hbtrie/internal/kverrors"
	"os"
)

const frameMaxNumberOfPages = 1000

// pagePosition returns the position of the page in the file.
func pagePosition(pageId uint64) uint64 {
	return frameMetaSize() + pageId*PageSize
}

// Frame is a self-managed unit of the buffer pool. It consists in a double linked list of pages.
// Each page, when queried, is pushed to the head of the list. Pages on the tail of the list are the least recently used.
// Pages on the tail should thus be evicted first.
type frame struct {
	// id    uint64
	head  *Page
	tail  *Page
	pages map[uint64]*Node
	// dirties    map[uint64]*Node
	cursor     uint64
	allocation uint64
	root       uint64
	size       uint64
	file       *os.File
}

// Pushes a non-existing page to the head of the frame.
func (l *frame) push(p *Page) {
	l.head.next.prev = p
	p.next = l.head.next
	p.prev = l.head
	l.head.next = p
}

// Removes a page from the frame.
func (l *frame) pop(p *Page) {
	p.prev.next = p.next
	p.next.prev = p.prev
}

// Pushes an existing page to the head of the frame.
func (l *frame) boost(p *Page) {
	l.pop(p)
	l.push(p)
}

// Initialises a new frame with a given file and an in-memory allocation.
func newFrame(file *os.File, allocation uint64) *frame {

	if allocation < 3 {
		panic("allocation for a frame must at least be of 3 pages")
	}

	l := &frame{
		head:       new(Page),
		tail:       new(Page),
		pages:      make(map[uint64]*Node),
		allocation: allocation,
		file:       file,
	}
	l.head.next = l.tail
	l.tail.prev = l.head
	l.pages[l.head.Id] = &Node{Page: l.head}
	l.pages[l.tail.Id] = &Node{Page: l.tail}
	l.cursor = 0
	return l
}

// Returns the node (page) with the given id.
func (l *frame) query(id uint64) *Node {

	if id < 1 {
		return nil
	}

	if p, ok := l.pages[id]; ok {

		l.boost(p.Page)
	}

	return l.pages[id]
}

// Returns a new node (page) and false if the frame has enough in-memory capacity. Otherwise, it returns nil and false.
func (l *frame) newNode() (node *Node, full bool) {
	if l.full() {
		return nil, true
	}
	l.cursor++
	if l.cursor > frameMaxNumberOfPages {
		panic("frame over page limit")
	}
	page := NewPage(l.cursor)
	node = &Node{Page: page}
	node.Dirty = true
	l.pages[node.Id] = node
	l.push(node.Page)
	return node, false
}

// States whether the frame has reached is in-memory capacity.
func (l *frame) full() bool {
	return len(l.pages) >= int(l.allocation)
}

// Adds a new page to the frame that was previously evicted.
func (l *frame) add(node *Node) error {
	if l.full() {
		return &kverrors.FrameOverflowError{Max: l.allocation}
	}
	if node.Id > l.cursor {
		return &kverrors.InvalidNodeIOError{Node: node.Id, Cursor: l.cursor}
	}
	if node.Id > frameMaxNumberOfPages {
		return &kverrors.InvalidNodeIOError{Node: node.Id, Cursor: frameMaxNumberOfPages}
	}
	l.pages[node.Id] = node
	l.push(node.Page)
	return nil
}

// evicts the least recently used page and returns it.
func (l *frame) evict() *Node {
	p := l.tail.prev
	l.pop(p)
	node := l.pages[p.Id]
	delete(l.pages, p.Id)
	return node
}

// Sets page id of the root b+ tree
func (l *frame) setRoot(pageId uint64) {
	l.root = pageId
}

// Returns page id of the root b+ tree
func (l *frame) getRoot() uint64 {
	return l.root
}

// Updates root and size in frame.
func (l *frame) update(root, size uint64) {
	l.root = root
	l.size = size
}
