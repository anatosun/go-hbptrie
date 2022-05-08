package bptree

import (
	"crypto/rand"
	"encoding/binary"
	"hbtrie/internal/pool"
	"unsafe"
)

type node struct {
	*pool.Page
	next             uint64
	prev             uint64
	children         [160]uint64
	entries          [160]entry
	numberOfChildren int
	numberOfEntries  int
}

func nodeHeaderLen() int {

	id := uint64(0)
	next := uint64(0)
	prev := uint64(0)

	return int(unsafe.Sizeof(id) + unsafe.Sizeof(next) + unsafe.Sizeof(prev))
}

func (n *node) insertChildAt(at int, child *node) error {
	if at < 0 || at > len(n.children) {
		return &IndexOutOfRangeError{Index: at, Length: len(n.children)}
	}

	copy(n.children[at+1:], n.children[at:])
	n.children[at] = child.Id
	n.numberOfChildren++
	return nil
}

func (n *node) full(l, c int) bool {
	if n.isLeaf() {
		return n.numberOfEntries == ((2 * l) - 1)
	}
	return n.numberOfEntries == ((2 * c) - 1)

}

// the two functions below implement both the BinaryMarshaler and the BinaryUnmarshaler interfaces
// refer to https://pkg.go.dev/encoding for more informations

func (n *node) MarshalBinary() ([]byte, error) {
	capacity := int(pool.PageSize) // 4KB
	buf := make([]byte, capacity)
	if _, err := rand.Read(buf); err != nil {
		return buf, err
	}
	bin := binary.LittleEndian
	bin.PutUint64(buf[0:8], n.Id)
	// buf[8] = byte(len(n.entries))  // 9th byte
	// buf[9] = byte(len(n.children)) // 10th byte (will be 0 for leaf)
	bin.PutUint64(buf[8:16], n.next)
	bin.PutUint64(buf[16:24], n.prev)
	cursor := 24
	if cursor != int(nodeHeaderLen()) {
		return buf, &InvalidSizeError{Got: cursor, Should: int(nodeHeaderLen())}
	}
	if n.isLeaf() {
		for _, e := range n.entries {
			eb, err := e.MarshalEntry()
			if err != nil {
				return nil, err
			}
			for j := 0; j < len(eb); j++ {
				buf[cursor+j] = eb[j]
			}
			cursor += entryLen()
			if cursor > capacity {
				return buf, &BufferOverflowError{Max: capacity, Cursor: cursor}
			}
		}
	}

	for _, c := range n.children {
		bin.PutUint64(buf[cursor:cursor+8], c)
		cursor += 8
		if cursor > capacity {
			return buf, &BufferOverflowError{Max: capacity, Cursor: cursor}
		}
	}

	if len(buf) != capacity {
		return buf, &InvalidSizeError{Got: len(buf), Should: capacity}
	}

	return buf, nil
}

func (n *node) UnmarshalBinary(data []byte) error {
	capacity := int(pool.PageSize) // 4KB
	if len(data) > capacity {
		return &InvalidSizeError{Got: len(data), Should: capacity}
	}
	n.Dirty = true
	bin := binary.LittleEndian
	n.Id = bin.Uint64(data[0:8])

	n.next = bin.Uint64(data[8:16])
	n.prev = bin.Uint64(data[16:24])
	cursor := 24
	if cursor != int(nodeHeaderLen()) {
		return &InvalidSizeError{Got: cursor, Should: int(nodeHeaderLen())}
	}
	for i := 0; i < len(n.entries); i++ {
		e := entry{}
		err := e.UnmarshalEntry(data[cursor : cursor+entryLen()])
		if err != nil {
			return err
		}
		n.entries[i] = e
		cursor += entryLen()
	}

	for i := 0; i < len(n.entries); i++ {
		n.children[i] = bin.Uint64(data[cursor : cursor+8])
		cursor += 8
	}

	return nil
}
