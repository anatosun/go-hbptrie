package pool

import (
	"encoding/binary"
	"hbtrie/internal/kverrors"
	"unsafe"
)

// Entry is the key-value unit of the bptree. it has a size of 25 bytes.
type Entry struct {
	IsTree  bool     // 1 byte
	Key     [16]byte // keys are chunks of 16 bytes
	FullKey []byte   // Full Key. Only inserted on the leaf
	Value   uint64   // values are pointers to subsequent b+ trees
}

func (e *Entry) EntryLen() int {
	b := true
	v := uint64(0)
	// 1 byte (IsTree), 16 byte (Key), 8 byte (Length of Full Key), dynamic size (Full Key), 8 byte (value)
	return int(unsafe.Sizeof(b)) + 16 + len(e.FullKey) + (2 * int(unsafe.Sizeof(v)))
}

func (e *Entry) MarshalEntry() ([]byte, error) {
	buf := make([]byte, e.EntryLen())
	if e.IsTree {
		buf[0] = 1
	}
	copy(buf[1:17], e.Key[:])
	// Store the length of the full key
	fullKeyLength := len(e.FullKey)
	binary.LittleEndian.PutUint64(buf[17:25], uint64(fullKeyLength))
	copy(buf[25:25+fullKeyLength], e.FullKey[:])
	binary.LittleEndian.PutUint64(buf[25+fullKeyLength:], e.Value)
	if len(buf) != e.EntryLen() {
		return nil, &kverrors.BufferOverflowError{Max: e.EntryLen(), Cursor: len(buf)}
	}
	return buf, nil
}

func (e *Entry) UnmarshalEntry(data []byte) error {
	if data[0] == 1 {
		e.IsTree = true
	}
	copy(e.Key[:], data[1:17])
	fullKeyLength := binary.LittleEndian.Uint64(data[17:25])
	offset := 25 + fullKeyLength
	e.FullKey = data[25:offset]
	e.Value = binary.LittleEndian.Uint64(data[offset : offset+8])
	return nil
}

func (e *Entry) Compare(f *Entry) (bool, string) {

	if e.IsTree != f.IsTree {
		return false, "IsTree"
	}
	if e.Value != f.Value {
		return false, "Value"
	}
	for i := 0; i < 16; i++ {
		if e.Key[i] != f.Key[i] {
			return false, "Key"
		}
	}
	return true, ""
}
