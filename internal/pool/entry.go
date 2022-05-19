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

func EntryLen() int {
	b := true
	v := uint64(0)
	return int(unsafe.Sizeof(b) + 16 + unsafe.Sizeof(v))
}

func (e *Entry) MarshalEntry() ([]byte, error) {
	buf := make([]byte, EntryLen())
	copy(buf[:16], e.Key[:])
	binary.LittleEndian.PutUint64(buf[16:], e.Value)
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
	e.Value = binary.LittleEndian.Uint64(data[16:])
	return nil
}
