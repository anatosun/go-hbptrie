package pool

import (
	"hbtrie/internal/kverrors"
	"hbtrie/internal/operations"
)

// States whether the given node is a leaf or an internal node.
func (n *Node) IsLeaf() bool {
	return n.NumberOfChildren == 0
}

// Inserts a new entry at the given index.
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

// Updates the value at the given index. If the value is not different, it does nothing.
func (n *Node) Update(at int, v uint64) error {
	if n.Entries[at].Value != v {
		n.Entries[at].Value = v
		n.Dirty = true
	}
	return nil
}

// Deletes an entry at the given index and shifts the remaining entries to the left.
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

// Searches for the given key in the node. The search is a classical binary search.
func (n *Node) Search(key [16]byte) (int, bool) {
	lower := 0
	upper := int(n.NumberOfEntries - 1)
	var cursor int
	for lower <= upper {
		cursor = (upper + lower) / 2
		cmp := n.Entries[cursor].Key

		if operations.Compare(key, cmp) == 0 {
			return cursor, true
		} else if operations.Compare(key, cmp) > 0 {
			lower = cursor + 1
		} else if operations.Compare(key, cmp) < 0 {
			upper = cursor - 1
		}
	}

	return lower, false
}
