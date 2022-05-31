package pool

import (
	"encoding/binary"
	"unsafe"
)

type metadata struct {
	root   uint64
	size   uint64
	cursor uint64
}

func metaSize() uint64 {
	return uint64(unsafe.Sizeof(metadata{}))
}

func (m *metadata) MarshalBinary() ([]byte, error) {
	buf := make([]byte, metaSize())
	bin := binary.LittleEndian
	bin.PutUint64(buf[0:8], m.root)
	bin.PutUint64(buf[8:16], m.size)
	bin.PutUint64(buf[16:24], m.cursor)
	return buf, nil
}

func (m *metadata) UnmarshalBinary(data []byte) error {
	bin := binary.LittleEndian
	m.root = bin.Uint64(data[0:8])
	m.size = bin.Uint64(data[8:16])
	m.cursor = bin.Uint64(data[16:24])

	return nil
}
