package bptree

import (
	"crypto/sha1"
	"testing"
)

var store *BPlusTree
var values map[[16]byte][8]byte

const size = 1000000

func TestInit(t *testing.T) {
	store = NewBplusTree()
	values = make(map[[16]byte][8]byte)
	h := sha1.New()

	for i := 0; i < size; i++ {
		h.Write([]byte{byte(i)})
		key := [16]byte{}
		copy(key[:], h.Sum(nil)[:16])
		value := [8]byte{}
		copy(value[:], h.Sum(nil)[:8])
		values[key] = value
	}

	if store.Len() != 0 {
		t.Errorf("size should be 0 but is %d", store.Len())
		t.FailNow()
	}

}

func TestInsert(t *testing.T) {
	//	t.Logf("inserting %d random keys", size)
	for key, value := range values {

		_, err := store.Insert(key, value)
		if err != nil {
			t.Errorf("while inserting to kv store(%d): %v", key, err)
			t.FailNow()
		}
	}

	expected := size
	actual := int(store.Len())

	if expected != actual {
		t.Errorf("expected %d, got %d", expected, actual)
		t.FailNow()
	}

}

// func TestUpdate(t *testing.T) {

// 	if store.Len() == 0 {
// 		TestInsert(t)
// 	}

// 	for key, value := range values {

// 		_, err := store.Search(key)
// 		if err != nil {
// 			t.Errorf("while searching for key %v: %v", key, err)
// 			t.FailNow()
// 		}
// 		success, err := store.Insert(key, value)

// 		if err != nil {
// 			t.Errorf("while inserting to kv store(%d): %v", key, err)
// 			t.FailNow()
// 		}

// 		if success {
// 			t.Errorf("should not be able to insert duplicate key: %v", key)
// 			t.FailNow()
// 		}

// 	}

// 	expected := size
// 	actual := int(store.Len())

// 	if expected != actual {
// 		t.Errorf("expected %d, got %d", expected, actual)
// 		t.FailNow()
// 	}
// }
