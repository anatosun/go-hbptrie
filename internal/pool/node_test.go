package pool

import (
	"testing"
)

func TestMarshalUnmarshalNode(t *testing.T) {

	node := Node{Page: NewPage(0)}
	node.Next = 4830759
	node.Prev = 48583
	offset := 29 // arbitrary offset
	for i := range node.Children {
		child := Node{Page: NewPage(uint64(i + offset))}
		node.InsertChildAt(i, &child)
	}

	data, err := node.MarshalBinary()
	if err != nil {
		t.Errorf("while marshaling: %v", err)
		t.FailNow()
	}

	u := Node{Page: NewPage(4789)}
	u.Next = 480
	u.Prev = 128
	err = u.UnmarshalBinary(data)

	if err != nil {
		t.Errorf("while unmarshaling: %v", err)
		t.FailNow()
	}

	if u.Id != node.Id {
		t.Errorf("expected %d, got %d", node.Id, u.Id)
		t.FailNow()
	}

	if len(u.Entries) != len(node.Entries) {
		t.Errorf("expected %d, got %d", len(node.Entries), len(u.Entries))
		t.FailNow()
	}

	if len(u.Children) != len(node.Children) {
		t.Errorf("expected %d, got %d", len(node.Children), len(u.Children))
		t.FailNow()
	}

	if u.Next != node.Next {
		t.Errorf("expected %d, got %d", node.Next, u.Next)
		t.FailNow()
	}

	if u.Prev != node.Prev {
		t.Errorf("expected %d, got %d", node.Prev, u.Prev)
		t.FailNow()
	}

	for i, e := range u.Entries {
		if e.Key != node.Entries[i].Key {
			t.Errorf("expected %d, got %d", node.Entries[i].Key, e.Key)
			t.FailNow()
		}
		if e.Value != node.Entries[i].Value {
			t.Errorf("expected %d, got %d", node.Entries[i].Value, e.Value)
			t.FailNow()
		}
	}

	for i, child := range u.Children {

		if child != node.Children[i] {
			t.Errorf("expected %d, got %d", node.Children[i], child)
			t.FailNow()
		}
	}

}

func TestMarshalUnmarshalNodeHalfFull(t *testing.T) {

	node := Node{Page: NewPage(0)}
	node.Next = 4830759
	node.Prev = 48583
	offset := 29 // arbitrary offset
	for i := 0; i <= len(node.Children)/2; i++ {
		child := Node{Page: NewPage(uint64(i + offset))}
		node.InsertChildAt(i, &child)
	}

	data, err := node.MarshalBinary()
	if err != nil {
		t.Errorf("while marshaling: %v", err)
		t.FailNow()
	}

	u := Node{Page: NewPage(4789)}
	u.Next = 480
	u.Prev = 128
	err = u.UnmarshalBinary(data)

	if err != nil {
		t.Errorf("while unmarshaling: %v", err)
		t.FailNow()
	}

	if u.Id != node.Id {
		t.Errorf("expected %d, got %d", node.Id, u.Id)
		t.FailNow()
	}

	if len(u.Entries) != len(node.Entries) {
		t.Errorf("expected %d, got %d", len(node.Entries), len(u.Entries))
		t.FailNow()
	}

	if len(u.Children) != len(node.Children) {
		t.Errorf("expected %d, got %d", len(node.Children), len(u.Children))
		t.FailNow()
	}

	if u.Next != node.Next {
		t.Errorf("expected %d, got %d", node.Next, u.Next)
		t.FailNow()
	}

	if u.Prev != node.Prev {
		t.Errorf("expected %d, got %d", node.Prev, u.Prev)
		t.FailNow()
	}

	for i, e := range u.Entries {
		if e.Key != node.Entries[i].Key {
			t.Errorf("expected %d, got %d", node.Entries[i].Key, e.Key)
			t.FailNow()
		}
		if e.Value != node.Entries[i].Value {
			t.Errorf("expected %d, got %d", node.Entries[i].Value, e.Value)
			t.FailNow()
		}
	}

	for i, child := range u.Children {

		if child != node.Children[i] {
			t.Errorf("expected %d, got %d", node.Children[i], child)
			t.FailNow()
		}
	}

}
