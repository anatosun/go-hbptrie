package pool

import (
	"encoding/binary"
	"hbtrie/internal/kverrors"
	"unsafe"
)

// Node is the unit of the B+ tree and is 3897 bytes long
type Node struct {
	*Page                        // 25 byte
	Next             uint64      // 8 byte
	Prev             uint64      // 8 byte
	Children         [120]uint64 // 960 byte
	Entries          [120]Entry  // 2880 byte
	NumberOfChildren uint64      // 8 byte
	NumberOfEntries  uint64      // 8 byte
}

func NodeHeaderLen() int {

	id := uint64(0)
	Next := uint64(0)
	Prev := uint64(0)
	NumberOfChildren := uint64(0)
	NumberOfEntries := uint64(0)

	return int(
		unsafe.Sizeof(id) +
			unsafe.Sizeof(Next) +
			unsafe.Sizeof(Prev) +
			unsafe.Sizeof(NumberOfChildren) +
			unsafe.Sizeof(NumberOfEntries))

}

func (n *Node) InsertChildAt(at int, child *Node) error {
	if at < 0 || at > len(n.Children) {
		return &kverrors.IndexOutOfRangeError{Index: at, Length: len(n.Children)}
	}

	copy(n.Children[at+1:], n.Children[at:])
	n.Children[at] = child.Id
	n.NumberOfChildren++
	n.Dirty = true
	return nil
}

// the two functions below implement both the BinaryMarshaler and the BinaryUnmarshaler interfaces
// refer to https://pkg.go.dev/encoding for more informations

func (n *Node) MarshalBinary() ([]byte, error) {
	capacity := int(PageSize) // 4KB
	buf := make([]byte, capacity)
	bin := binary.LittleEndian
	bin.PutUint64(buf[0:8], n.Id)
	bin.PutUint64(buf[8:16], n.NumberOfEntries)
	bin.PutUint64(buf[16:24], n.NumberOfChildren)
	// if n.NumberOfEntries > 0 && n.NumberOfChildren > 0 {
	// 	return buf, &kverrors.InvalidNodeSizeError{NumberOfChildren: n.NumberOfChildren, NumberOfEntries: n.NumberOfEntries}
	// }
	bin.PutUint64(buf[24:32], n.Next)
	bin.PutUint64(buf[32:40], n.Prev)

	cursor := 40
	if cursor != int(NodeHeaderLen()) {
		return buf, &kverrors.InvalidSizeError{Got: cursor, Should: int(NodeHeaderLen())}
	}

	for i := 0; i < int(n.NumberOfEntries); i++ {
		e := n.Entries[i]
		eb, err := e.MarshalEntry()
		if err != nil {
			return nil, err
		}
		for j := 0; j < len(eb); j++ {
			buf[cursor+j] = eb[j]
		}
		cursor += EntryLen()
		if cursor > capacity {
			return buf, &kverrors.BufferOverflowError{Max: capacity, Cursor: cursor}
		}
	}

	for i := 0; i < int(n.NumberOfChildren); i++ {
		c := n.Children[i]
		bin.PutUint64(buf[cursor:cursor+8], c)
		cursor += 8
		if cursor > capacity {
			return buf, &kverrors.BufferOverflowError{Max: capacity, Cursor: cursor}
		}
	}

	if len(buf) != capacity {
		return buf, &kverrors.InvalidSizeError{Got: len(buf), Should: capacity}
	}

	return buf, nil
}

func (n *Node) UnmarshalBinary(data []byte) error {
	capacity := int(PageSize) // 4KB
	if len(data) > capacity {
		return &kverrors.InvalidSizeError{Got: len(data), Should: capacity}
	}
	n.Dirty = false
	bin := binary.LittleEndian
	n.Id = bin.Uint64(data[0:8])
	n.NumberOfEntries = bin.Uint64(data[8:16])
	n.NumberOfChildren = bin.Uint64(data[16:24])
	// if n.NumberOfEntries > 0 && n.NumberOfChildren > 0 {
	// 	return &kverrors.InvalidNodeSizeError{NumberOfChildren: n.NumberOfChildren, NumberOfEntries: n.NumberOfEntries}
	// }
	n.Next = bin.Uint64(data[24:32])
	n.Prev = bin.Uint64(data[32:40])

	cursor := 40
	if cursor != int(NodeHeaderLen()) {
		return &kverrors.InvalidSizeError{Got: cursor, Should: int(NodeHeaderLen())}
	}
	if n.NumberOfEntries > uint64(len(n.Entries)) {
		return &kverrors.OverflowError{Type: "Number of entries", Actual: n.NumberOfEntries, Max: len(n.Entries)}
	}
	for i := 0; i < int(n.NumberOfEntries); i++ {
		e := Entry{}
		err := e.UnmarshalEntry(data[cursor : cursor+EntryLen()])
		if err != nil {
			return err
		}
		n.Entries[i] = e
		cursor += EntryLen()
	}
	if n.NumberOfChildren > uint64(len(n.Children)) {
		return &kverrors.OverflowError{Type: "Number of children", Actual: n.NumberOfChildren, Max: len(n.Children)}
	}
	for i := 0; i < int(n.NumberOfChildren); i++ {
		n.Children[i] = bin.Uint64(data[cursor : cursor+8])
		cursor += 8
	}

	return nil
}
