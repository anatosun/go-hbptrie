package bptree

import (
	"crypto/sha1"
	"hbtrie/internal/pool"
	"math/rand"
	"testing"
)

var store *BPlusTree
var values map[[16]byte]uint64

const size = 8000

func TestInit(t *testing.T) {
	store = NewBplusTree(pool.NewBufferpool(nil, uint64(size)))
	values = make(map[[16]byte]uint64)
	h := sha1.New()

	for i := 0; i < size; i++ {
		h.Write([]byte{byte(i)})
		key := [16]byte{}
		copy(key[:], h.Sum(nil)[:16])
		value := rand.Uint64()
		values[key] = value
	}

	if store.Len() != 0 {
		t.Errorf("expected size %d, got %d", 0, store.Len())
		t.FailNow()
	}

}

func TestInsert(t *testing.T) {
	step := 0
	for key, value := range values {

		success, err := store.Insert(key, value)
		if err != nil {
			t.Errorf("[step %d] while inserting to kv store(%d): %v", step, key, err)
			t.FailNow()
		}

		if !success {
			t.Errorf("[step %d] should be able to insert key: %v", step, key)
			t.FailNow()
		}

		v, err := store.Search(key)
		if err != nil {
			t.Errorf("[step %d] while searching for key '%v': %v", step, key, err)
			t.FailNow()
		}

		if v == value {
			t.Errorf("[step %d] expected %d, got %d", step, value, v)
			t.FailNow()
		}
	}

	expected := len(values)
	actual := int(store.Len())

	if expected != actual {
		t.Errorf("expected %d, got %d", expected, actual)
		t.FailNow()
	}

}

func TestUpdate(t *testing.T) {

	step := 0
	for key, value := range values {

		_, err := store.Search(key)
		if err != nil {
			t.Errorf("[step %d] while searching for key '%v': %v", step, key, err)
			t.FailNow()
		}
		success, err := store.Insert(key, value)

		if err != nil {
			t.Errorf("[step %d] while inserting to kv store(%d): %v", step, key, err)
			t.FailNow()
		}

		if success {
			t.Errorf("[step %d] should not be able to insert duplicate key: %v", step, key)
			t.FailNow()
		}
		step++
	}

	expected := len(values)
	actual := int(store.Len())

	if expected != actual {
		t.Errorf("expected size %d, got %d", expected, actual)
		t.FailNow()
	}
}
