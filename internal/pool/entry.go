package pool

import (
	"encoding/binary"
	"fmt"
	"hbtrie/internal/kverrors"
	"unsafe"
)

// Entry is the key-value unit of the bptree. it has a size of 25 bytes.
type Entry struct {
	IsTree bool     // 1 byte
	Key    [16]byte // keys are chunks of 16 bytes
	Value  uint64   // values are pointers to subsequent b+ trees
}

// Returns the byte length of one entry.
func EntryLen() int {
	b := true
	v := uint64(0)
	return int(unsafe.Sizeof(b) + 16 + unsafe.Sizeof(v))
}

// Implements the binary.BinaryMarshaler interface.
func (e *Entry) MarshalBinary() ([]byte, error) {
	buf := make([]byte, EntryLen())
	if e.IsTree {
		buf[0] = 1
	}
	copy(buf[1:17], e.Key[:])
	binary.LittleEndian.PutUint64(buf[17:], e.Value)
	if len(buf) != EntryLen() {
		return nil, &kverrors.BufferOverflowError{Max: EntryLen(), Cursor: len(buf)}
	}
	return buf, nil
}

// Implements the binary.BinaryUnmarshaler interface.
func (e *Entry) UnmarshalBinary(data []byte) error {
	if len(data) != EntryLen() {
		return fmt.Errorf("invalid Entry size: %d", len(data))
	}
	if data[0] == 1 {
		e.IsTree = true
	}
	copy(e.Key[:], data[1:17])
	e.Value = binary.LittleEndian.Uint64(data[17:])
	return nil
}
