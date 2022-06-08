package pool

import (
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
