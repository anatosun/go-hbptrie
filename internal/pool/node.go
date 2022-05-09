package pool

import (
	"crypto/rand"
	"encoding/binary"
	"hbtrie/internal/kverrors"
	"unsafe"
)

// Node is the unit of the B+ tree and is 4843 bytes long
type Node struct {
	*Page                        // 25 byte
	Next             uint64      // 8 byte
	Prev             uint64      // 8 byte
	Children         [150]uint64 // 1200 byte
	Entries          [150]Entry  // 3600 byte
	NumberOfChildren int
	NumberOfEntries  int
}

func NodeHeaderLen() int {

	id := uint64(0)
	Next := uint64(0)
	Prev := uint64(0)

	return int(unsafe.Sizeof(id) + unsafe.Sizeof(Next) + unsafe.Sizeof(Prev))
}

func (n *Node) InsertChildAt(at int, child *Node) error {
	if at < 0 || at > len(n.Children) {
		return &kverrors.IndexOutOfRangeError{Index: at, Length: len(n.Children)}
	}

	copy(n.Children[at+1:], n.Children[at:])
	n.Children[at] = child.Id
	n.NumberOfChildren++
	return nil
}

// the two functions below implement both the BinaryMarshaler and the BinaryUnmarshaler interfaces
// refer to https://pkg.go.dev/encoding for more informations

func (n *Node) MarshalBinary() ([]byte, error) {
	capacity := int(PageSize) // 4KB
	buf := make([]byte, capacity)
	if _, err := rand.Read(buf); err != nil {
		return buf, err
	}
	bin := binary.LittleEndian
	bin.PutUint64(buf[0:8], n.Id)
	// buf[8] = byte(len(n.entries))  // 9th byte
	// buf[9] = byte(len(n.Children)) // 10th byte (will be 0 for leaf)
	bin.PutUint64(buf[8:16], n.Next)
	bin.PutUint64(buf[16:24], n.Prev)
	cursor := 24
	if cursor != int(NodeHeaderLen()) {
		return buf, &kverrors.InvalidSizeError{Got: cursor, Should: int(NodeHeaderLen())}
	}
	if n.IsLeaf() {
		for _, e := range n.Entries {
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
	}

	for _, c := range n.Children {
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
	n.Dirty = true
	bin := binary.LittleEndian
	n.Id = bin.Uint64(data[0:8])

	n.Next = bin.Uint64(data[8:16])
	n.Prev = bin.Uint64(data[16:24])
	cursor := 24
	if cursor != int(NodeHeaderLen()) {
		return &kverrors.InvalidSizeError{Got: cursor, Should: int(NodeHeaderLen())}
	}
	for i := 0; i < len(n.Entries); i++ {
		e := Entry{}
		err := e.UnmarshalEntry(data[cursor : cursor+EntryLen()])
		if err != nil {
			return err
		}
		n.Entries[i] = e
		cursor += EntryLen()
	}

	for i := 0; i < len(n.Entries); i++ {
		n.Children[i] = bin.Uint64(data[cursor : cursor+8])
		cursor += 8
	}

	return nil
}
