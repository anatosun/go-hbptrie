package pool

import (
	"hbtrie/internal/kverrors"
	"hbtrie/internal/operations"
)

func (n *Node) IsLeaf() bool {
	return n.NumberOfChildren == 0
}

func (n *Node) InsertEntryAt(at int, e Entry) error {

	if at < 0 || at > len(n.Entries)-1 {
		return &kverrors.IndexOutOfRangeError{Index: at, Length: len(n.Entries)}
	}
	copy(n.Entries[at+1:], n.Entries[at:])
	n.Entries[at] = e
	n.NumberOfEntries++
	n.Dirty = true
	return nil
}

func (n *Node) Update(at int, v uint64) error {
	if n.Entries[at].Value != v {
		n.Entries[at].Value = v
		n.Dirty = true
	}
	return nil
}

func (n *Node) DeleteEntryAt(at int) (Entry, error) {
	if at < 0 || at > len(n.Entries) {
		return Entry{}, &kverrors.IndexOutOfRangeError{Index: at, Length: len(n.Entries)}
	}
	entry := n.Entries[at]
	copy(n.Entries[at:], n.Entries[at+1:])
	n.NumberOfEntries--
	n.Dirty = true
	return entry, nil
}

func (n *Node) Search(key [16]byte) (int, bool) {
	lower := 0
	upper := int(n.NumberOfEntries - 1)
	var cursor int
	for lower <= upper {
		cursor = (upper + lower) / 2
		cmp := n.Entries[cursor].Key

		if operations.Equal(key, cmp) {
			return cursor, true
		} else if operations.Compare(key, cmp) > 0 {
			lower = cursor + 1
		} else if operations.Compare(key, cmp) < 0 {
			upper = cursor - 1
		}
	}

	return lower, false
}
