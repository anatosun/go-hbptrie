package pool

import (
	"math/rand"
	"testing"
)

func TestMarshalBinary(t *testing.T) {
	meta := &metadata{}
	meta.root = tuple{frame: 1, root: rand.Uint64()}
	for i := 2; i < limit; i++ {
		meta.children = append(meta.children, tuple{frame: uint64(i), root: rand.Uint64()})
	}

	data, err := meta.MarshalBinary()
	if err != nil {
		t.Errorf("while marshaling metadata: %v", err)
		t.FailNow()
	}

	if len(data) != metaSize() {
		t.Errorf("expected %d, got %d", metaSize(), len(data))
		t.FailNow()
	}

	meta2 := &metadata{}
	err = meta2.UnmarshalBinary(data)
	if err != nil {
		t.Errorf("while unmarshaling metadata: %v", err)
		t.FailNow()
	}

	if meta.root.frame != meta2.root.frame {
		t.Errorf("expected %d, got %d", meta.root.frame, meta2.root.frame)
		t.FailNow()
	}
	if meta.root.root != meta2.root.root {
		t.Errorf("expected %d, got %d", meta.root.root, meta2.root.root)
		t.FailNow()
	}

	if len(meta.children) != len(meta2.children) {
		t.Errorf("expected %d, got %d", len(meta.children), len(meta2.children))
		t.FailNow()
	}

	for i, child := range meta.children {
		if child.frame != meta2.children[i].frame {
			t.Errorf("expected %d, got %d", meta.children[i].frame, meta2.children[i].frame)
			t.FailNow()
		}
		if child.root != meta2.children[i].root {
			t.Errorf("expected %d, got %d", meta.children[i].root, meta2.children[i].root)
			t.FailNow()
		}
	}
}
