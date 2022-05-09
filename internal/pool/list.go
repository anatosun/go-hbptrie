package pool

type List struct {
	head    *Page
	tail    *Page
	pages   map[uint64]*Node
	dirties map[uint64]*Node
	cursor  uint64
}

func (l *List) push(p *Page) {
	l.head.next.prev = p
	p.next = l.head.next
	p.prev = l.head
	l.head.next = p
}

func (l *List) pop(p *Page) {
	p.prev.next = p.next
	p.next.prev = p.prev
}

func (l *List) boost(p *Page) {
	l.pop(p)
	l.push(p)
}

const preaollocation = 1000 * 1000

func NewList() *List {
	l := &List{
		head:    NewPage(1),
		tail:    NewPage(preaollocation),
		pages:   make(map[uint64]*Node),
		dirties: make(map[uint64]*Node),
	}
	l.head.next = l.tail
	l.tail.prev = l.head
	l.cursor = 2
	return l
}

func (l *List) Query(id uint64) *Node {

	if p, ok := l.pages[id]; ok {
		l.boost(p.Page)
	}

	return l.pages[id]
}

func (l *List) NewNode() (*Node, error) {
	l.cursor++
	page := NewPage(l.cursor)
	n := &Node{Page: page}
	l.pages[n.Id] = n
	l.push(n.Page)
	return n, nil
}
