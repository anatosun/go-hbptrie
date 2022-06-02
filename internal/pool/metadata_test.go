package pool

import (
	"math/rand"
	"testing"
)

func TestMarshalUnmarshalFrameMetadata(t *testing.T) {
	meta := &frameMetadata{}
	meta.root = rand.Uint64()
	meta.size = rand.Uint64()
	if frameMetaSize() != 24 {
		t.Errorf("expected 24, got %d", frameMetaSize())
		t.FailNow()
	}
	data, err := meta.MarshalBinary()
	if err != nil {
		t.Errorf("while marshalling: %v", err)
		t.FailNow()
	}
	meta2 := &frameMetadata{}
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
	if meta.cursor != meta2.cursor {
		t.Errorf("expected %d, got %d", meta.cursor, meta2.cursor)
		t.FailNow()
	}

}

func TestMarshalUnmarshalHBMeta(t *testing.T) {
	meta := &hbMetatadata{}
	meta.root = rand.Uint64()
	meta.size = rand.Uint64()
	meta.nframes = rand.Uint64()
	if hbMetaSize() != 24 {
		t.Errorf("expected 24, got %d", hbMetaSize())
		t.FailNow()
	}
	data, err := meta.MarshalBinary()
	if err != nil {
		t.Errorf("while marshalling: %v", err)
		t.FailNow()
	}
	meta2 := &hbMetatadata{}
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
	if meta.nframes != meta2.nframes {
		t.Errorf("expected %d, got %d", meta.nframes, meta2.nframes)
		t.FailNow()
	}
}
