package pool

import (
	"fmt"
	"hbtrie/internal/kverrors"
	"unsafe"
)

// Entry is the key-value unit of the bptree. it has a size of 24 bytes.
type Entry struct {
	IsTree bool     // 1 byte
	Key    [16]byte // keys are chunks of 16 bytes
	Value  [8]byte  // values are pointers to subsequent b+ trees
}

func EntryLen() int {
	return int(unsafe.Sizeof(Entry{}))
}

func (e *Entry) MarshalEntry() ([]byte, error) {
	buf := make([]byte, EntryLen())
	copy(buf[:16], e.Key[:])
	copy(buf[16:], e.Value[:])
	if len(buf) != EntryLen() {
		return nil, &kverrors.BufferOverflowError{Max: EntryLen(), Cursor: len(buf)}
	}
	return buf, nil
}

func (e *Entry) UnmarshalEntry(data []byte) error {
	if len(data) != EntryLen() {
		return fmt.Errorf("invalid Entry size: %d", len(data))
	}
	copy(e.Key[:], data[:16])
	copy(e.Value[:], data[16:])
	return nil
}
