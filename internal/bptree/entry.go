package bptree

import (
	"fmt"
	"unsafe"
)

// entry is the key-value unit of the bptree. it has a size of 24 bytes.
type entry struct {
	key   [16]byte // keys are chunks of 16 bytes
	value [8]byte  // values are pointers to subsequent b+ trees
}

func entryLen() int {
	return int(unsafe.Sizeof(entry{}))
}

func (e *entry) MarshalEntry() ([]byte, error) {
	buf := make([]byte, entryLen())
	copy(buf[:16], e.key[:])
	copy(buf[16:], e.value[:])
	if len(buf) != entryLen() {
		return nil, &BufferOverflowError{Max: entryLen(), Cursor: len(buf)}
	}
	return buf, nil
}

func (e *entry) UnmarshalEntry(data []byte) error {
	if len(data) != entryLen() {
		return fmt.Errorf("invalid entry size: %d", len(data))
	}
	copy(e.key[:], data[:16])
	copy(e.value[:], data[16:])
	return nil
}
