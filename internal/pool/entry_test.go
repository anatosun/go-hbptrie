package pool

import (
	"math"
	"testing"
	"unsafe"
)

func TestMarshalUnmarshalEntry(t *testing.T) {
	testBool := true
	if unsafe.Sizeof(testBool) != 1 {
		t.Errorf("expected size %d, got %d", 1, unsafe.Sizeof(testBool))
		t.FailNow()
	}

	if EntryLen() != 25 {
		t.Errorf("expected 25, got %d", EntryLen())
		t.FailNow()
	}

	k := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 15}
	v := uint64(math.MaxUint64)
	e := Entry{Key: k, Value: v, IsTree: true}
	data, err := e.MarshalEntry()
	if err != nil {
		t.Errorf("while marshaling: %v", err)
		t.FailNow()
	}
	u := Entry{Key: [16]byte{1, 2, 3, 2, 5, 21, 7, 56, 9, 255, 21, 13, 13, 14, 15, 15}, Value: 0, IsTree: false}
	err = u.UnmarshalEntry(data)
	if err != nil {
		t.Errorf("while unmarshaling: %v", err)
		t.FailNow()
	}
	if e.IsTree != u.IsTree {
		t.Errorf("expected %t, got %t", e.IsTree, u.IsTree)
		t.FailNow()
	}
	if u.Key != k {
		t.Errorf("expected %d, got %d", k, u.Key)
		t.FailNow()
	}
	if u.Value != v {
		t.Errorf("expected %d, got %d", v, u.Value)
		t.FailNow()
	}

	u = *new(Entry)
	err = u.UnmarshalEntry(data)
	if err != nil {
		t.Errorf("while unmarshaling: %v", err)
		t.FailNow()
	}
	if e.IsTree != u.IsTree {
		t.Errorf("expected %t, got %t", e.IsTree, u.IsTree)
		t.FailNow()
	}
	if u.Key != k {
		t.Errorf("expected %d, got %d", k, u.Key)
		t.FailNow()
	}
	if u.Value != v {
		t.Errorf("expected %d, got %d", v, u.Value)
		t.FailNow()
	}
}
