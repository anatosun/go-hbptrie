package pool

type list struct {
	head    *Page
	tail    *Page
	pages   map[uint64]*Page
	dirties map[uint64]*Page
}

func (l *list) push(p *Page) {
	l.head.next.prev = p
	p.next = l.head.next
	p.prev = l.head
	l.head.next = p
}

func (l *list) pop(p *Page) {
	p.prev.next = p.next
	p.next.prev = p.prev
}

func (l *list) boost(p *Page) {
	l.pop(p)
	l.push(p)
}

func newList() *list {
	l := &list{
		head:    NewPage(),
		tail:    NewPage(),
		pages:   make(map[uint64]*Page),
		dirties: make(map[uint64]*Page),
	}
	l.head.next = l.tail
	l.tail.prev = l.head
	return l
}
