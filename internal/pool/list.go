package pool

import "fmt"

type List struct {
	head    *Page
	tail    *Page
	pages   map[uint64]*Page
	dirties map[uint64]*Page
	cursor  int
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
		pages:   make(map[uint64]*Page),
		dirties: make(map[uint64]*Page),
	}
	l.head.next = l.tail
	l.tail.prev = l.head
	l.cursor = 2
	return l
}

func (l *List) Query(id uint64) *Page {
	fmt.Println("query of ", id)

	return l.pages[id]
}

func (l *List) Allocate() (*Page, error) {
	l.cursor++
	p := NewPage(uint64(l.cursor))
	l.pages[p.Id] = p
	l.push(p)
	return p, nil
}
