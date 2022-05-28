package pool

import (
	"math"
	"testing"
)

func TestMarshalUnmarshalMetadata(t *testing.T) {
	meta := &metadata{}
	meta.root = math.MaxUint64
	meta.size = math.MaxUint64 - 4739
	data, err := meta.MarshalBinary()
	if err != nil {
		t.Errorf("while marshalling: %v", err)
		t.FailNow()
	}
	meta2 := &metadata{}
	err = meta2.UnmarshalBinary(data)
	if err != nil {
		t.Errorf("while unmarshalling: %v", err)
		t.FailNow()
	}
	if meta.root != meta2.root {
		t.Errorf("expected %d, got %d", meta.root, meta2.root)
		t.FailNow()
	}
	if meta.size != meta2.size {
		t.Errorf("expected %d, got %d", meta.size, meta2.size)
		t.FailNow()
	}

}
