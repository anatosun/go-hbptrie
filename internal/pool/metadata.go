package pool

import (
	"encoding/binary"
	"unsafe"
)

type frameMetadata struct {
	root   uint64
	size   uint64
	cursor uint64
}

func frameMetaSize() uint64 {
	return uint64(unsafe.Sizeof(frameMetadata{}))
}

func (m *frameMetadata) MarshalBinary() ([]byte, error) {
	buf := make([]byte, frameMetaSize())
	bin := binary.LittleEndian
	bin.PutUint64(buf[0:8], m.root)
	bin.PutUint64(buf[8:16], m.size)
	bin.PutUint64(buf[16:24], m.cursor)
	return buf, nil
}

func (m *frameMetadata) UnmarshalBinary(data []byte) error {
	bin := binary.LittleEndian
	m.root = bin.Uint64(data[0:8])
	m.size = bin.Uint64(data[8:16])
	m.cursor = bin.Uint64(data[16:24])

	return nil
}

type hbMetatadata struct {
	root    uint64
	size    uint64
	nframes uint64
}

func hbMetaSize() uint64 {
	return uint64(unsafe.Sizeof(hbMetatadata{}))
}

func (m *hbMetatadata) MarshalBinary() ([]byte, error) {
	buf := make([]byte, hbMetaSize())
	bin := binary.LittleEndian
	bin.PutUint64(buf[0:8], m.root)
	bin.PutUint64(buf[8:16], m.size)
	bin.PutUint64(buf[16:24], m.nframes)
	return buf, nil
}

func (m *hbMetatadata) UnmarshalBinary(data []byte) error {
	bin := binary.LittleEndian
	m.root = bin.Uint64(data[0:8])
	m.size = bin.Uint64(data[8:16])
	m.nframes = bin.Uint64(data[16:24])

	return nil
}
