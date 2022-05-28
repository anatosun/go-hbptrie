package pool

import (
	"hbtrie/internal/kverrors"
)

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
	rootPageId uint64
}

func (l *frame) push(p *Page) {
	l.head.next.prev = p
	p.next = l.head.next
	p.prev = l.head
	l.head.next = p
}

func (l *frame) pop(p *Page) {
	p.prev.next = p.next
	p.next.prev = p.prev
}

func (l *frame) boost(p *Page) {
	if p.Id == l.tail.Id {
		return
	}
	l.pop(p)
	l.push(p)
}

func newFrame(allocation uint64) *frame {

	if allocation < 3 {
		panic("allocation for a frame must at least be of 3 pages")
	}

	l := &frame{
		head:       new(Page),
		tail:       new(Page),
		pages:      make(map[uint64]*Node),
		allocation: allocation,
	}
	l.head.next = l.tail
	l.tail.prev = l.head
	l.pages[l.head.Id] = &Node{Page: l.head}
	l.pages[l.tail.Id] = &Node{Page: l.tail}
	l.cursor = 0
	return l
}

func (l *frame) query(id uint64) *Node {

	if id < 1 {
		return nil
	}

	if p, ok := l.pages[id]; ok {

		l.boost(p.Page)
	}

	return l.pages[id]
}

func (l *frame) newNode() (node *Node, full bool) {
	if l.full() {
		return nil, true
	}
	l.cursor++
	page := NewPage(l.cursor)
	n := &Node{Page: page}
	l.pages[n.Id] = n
	l.push(n.Page)
	return n, false
}

func (l *frame) full() bool {
	return len(l.pages) >= int(l.allocation)
}

// add a new page to the frame that was previously evicted.
func (l *frame) add(node *Node) error {
	if l.full() {
		return &kverrors.FrameOverflowError{Max: l.allocation}
	}
	if node.Id > l.cursor {
		return &kverrors.InvalidNodeError{}
	}
	l.pages[node.Id] = node
	l.push(node.Page)
	return nil
}

// for debuggin purposes
// func (l *frame) printLinkedList() {
// 	p := l.head
// 	for p != nil {
// 		fmt.Printf("%d ", p.Id)
// 		p = p.next
// 	}
// 	fmt.Println()
// }

func (l *frame) evict() *Node {
	p := l.tail.prev
	l.pop(p)
	node := l.pages[p.Id]
	delete(l.pages, p.Id)
	return node
}

// Sets page id of the root b+ tree
func (l *frame) setRootPageId(pageId uint64) {
	l.rootPageId = pageId
}

// Returns page id of the root b+ tree
func (l *frame) getRootPageId() uint64 {
	return l.rootPageId
}
