package pool

import "hbtrie/internal/kverrors"

// Frame is a self-managed unit of the buffer pool. It consists in a double linked list of pages.
// Each page, when queried, is pushed to the head of the list. Pages on the tail of the list are the least recently used.
// Pages on the tail should thus be evicted first.
type Frame struct {
	// id    uint64
	head  *Page
	tail  *Page
	pages map[uint64]*Node
	// dirties    map[uint64]*Node
	cursor     uint64
	allocation uint64
}

func (l *Frame) push(p *Page) {
	l.head.next.prev = p
	p.next = l.head.next
	p.prev = l.head
	l.head.next = p
}

func (l *Frame) pop(p *Page) {
	p.prev.next = p.next
	p.next.prev = p.prev
}

func (l *Frame) boost(p *Page) {
	l.pop(p)
	l.push(p)
}

func NewFrame(allocation uint64) *Frame {
	l := &Frame{
		head:  NewPage(1),
		tail:  NewPage(allocation),
		pages: make(map[uint64]*Node),
		// dirties: make(map[uint64]*Node),
	}
	l.allocation = allocation
	l.head.next = l.tail
	l.tail.prev = l.head
	l.cursor = 1
	return l
}

func (l *Frame) Query(id uint64) *Node {

	if p, ok := l.pages[id]; ok {
		l.boost(p.Page)
	}

	return l.pages[id]
}

func (l *Frame) NewNode() (*Node, error) {
	if l.cursor >= l.allocation-1 {
		return nil, &kverrors.BufferOverflowError{Max: l.allocation, Cursor: l.cursor}
	}
	l.cursor++
	page := NewPage(l.cursor)
	n := &Node{Page: page}
	l.pages[n.Id] = n
	l.push(n.Page)
	return n, nil
}
