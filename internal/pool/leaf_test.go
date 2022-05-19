package pool

import (
	"crypto/sha1"
	"math/rand"
	"testing"
)

func TestMarshalUnmarshalLeaf(t *testing.T) {
	degree := uint8(rand.Int() % 70)
	leaf := &Node{Page: NewPage(0)}
	leaf.Next = 48307593
	leaf.Prev = 485830
	h := sha1.New()

	for i := 0; i < int(degree*2); i++ {
		h.Write([]byte{byte(i)})
		key := [16]byte{}
		copy(key[:], h.Sum(nil)[:16])
		value := rand.Uint64()
		entry := Entry{Key: key, Value: value}
		leaf.InsertEntryAt(i, entry)
	}

	data, err := leaf.MarshalBinary()
	if err != nil {
		t.Errorf("while marshaling: %v", err)
		t.FailNow()
	}

	u := &Node{Page: NewPage(4789)}
	u.Next = 480
	u.Prev = 128
	err = u.UnmarshalBinary(data)

	if err != nil {
		t.Errorf("while unmarshaling: %v", err)
		t.FailNow()
	}

	if u.Id != leaf.Id {
		t.Errorf("expected %d, got %d", leaf.Id, u.Id)
		t.FailNow()
	}

	if len(u.Entries) != len(leaf.Entries) {
		t.Errorf("expected %d, got %d", len(leaf.Entries), len(u.Entries))
		t.FailNow()
	}

	if len(u.Children) != len(leaf.Children) {
		t.Errorf("expected %d, got %d", len(leaf.Children), len(u.Children))
		t.FailNow()
	}

	if u.Next != leaf.Next {
		t.Errorf("expected %d, got %d", leaf.Next, u.Next)
		t.FailNow()
	}

	if u.Prev != leaf.Prev {
		t.Errorf("expected %d, got %d", leaf.Prev, u.Prev)
		t.FailNow()
	}

	for i, e := range u.Entries {
		if e.Key != leaf.Entries[i].Key {
			t.Errorf("expected %d, got %d", leaf.Entries[i].Key, e.Key)
			t.FailNow()
		}
		if e.Value != leaf.Entries[i].Value {
			t.Errorf("expected %d, got %d", leaf.Entries[i].Value, e.Value)
			t.FailNow()
		}
	}

}
