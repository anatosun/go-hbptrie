package bptree

func (n *node) isLeaf() bool {
	return n.numberOfChildren == 0
}

func (n *node) insertEntryAt(at int, e entry) error {

	if at < 0 || at > len(n.entries)-1 {
		return &IndexOutOfRangeError{Index: at, Length: len(n.entries)}
	}
	copy(n.entries[at+1:], n.entries[at:])
	n.entries[at] = e
	n.numberOfEntries++
	return nil
}

func (n *node) update(at int, value [8]byte) error {
	if n.entries[at].value != value {
		n.entries[at].value = value
	}
	return nil
}

func (n *node) deleteEntryAt(at int) (entry, error) {
	if at < 0 || at > len(n.entries) {
		return entry{}, &IndexOutOfRangeError{Index: at, Length: len(n.entries)}
	}
	entry := n.entries[at]
	copy(n.entries[at:], n.entries[at+1:])
	n.numberOfEntries--
	return entry, nil
}

func (n *node) search(key [16]byte) (int, bool) {
	lower := 0
	upper := n.numberOfEntries - 1
	var cursor int
	for lower <= upper {
		cursor = (upper + lower) / 2
		cmp := n.entries[cursor].key

		if Compare(key, cmp) == 0 {
			return cursor, true
		} else if Compare(key, cmp) > 0 {
			lower = cursor + 1
		} else if Compare(key, cmp) < 0 {
			upper = cursor - 1
		}
	}

	return lower, false
}
