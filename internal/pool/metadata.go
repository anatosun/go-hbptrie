package pool

import (
	"encoding/binary"
	"hbtrie/internal/kverrors"
)

type tuple struct {
	frame uint64
	root  uint64
}

type metadata struct {
	root     tuple
	children []tuple
}

func metaSize() int {
	return 8 + 8 + (8+8)*limit
}

func (m *metadata) MarshalBinary() ([]byte, error) {
	buf := make([]byte, metaSize())
	bin := binary.LittleEndian
	bin.PutUint64(buf[0:8], m.root.frame)
	bin.PutUint64(buf[8:16], m.root.root)
	bin.PutUint64(buf[16:24], uint64(len(m.children)))
	cursor := 24
	for _, child := range m.children {
		bin.PutUint64(buf[cursor:cursor+8], child.frame)
		cursor += 8
		if cursor > metaSize() {
			return buf, &kverrors.BufferOverflowError{Max: metaSize(), Cursor: cursor}
		}
		bin.PutUint64(buf[cursor:cursor+8], child.root)
		cursor += 8
		if cursor > metaSize() {
			return buf, &kverrors.BufferOverflowError{Max: metaSize(), Cursor: cursor}
		}

	}

	return buf, nil
}

func (m *metadata) UnmarshalBinary(data []byte) error {
	bin := binary.LittleEndian
	m.root.frame = bin.Uint64(data[0:8])
	m.root.root = bin.Uint64(data[8:16])
	size := int(bin.Uint64(data[16:24]))
	cursor := 24
	for i := 0; i < size; i++ {
		child := tuple{}
		child.frame = bin.Uint64(data[cursor : cursor+8])
		cursor += 8
		if cursor > metaSize() {
			return &kverrors.BufferOverflowError{Max: metaSize(), Cursor: cursor}
		}
		child.root = bin.Uint64(data[cursor : cursor+8])
		cursor += 8
		if cursor > metaSize() {
			return &kverrors.BufferOverflowError{Max: metaSize(), Cursor: cursor}
		}
		m.children = append(m.children, child)
	}

	return nil
}
