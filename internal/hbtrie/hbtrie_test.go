package hbtrie

import (
	"bytes"
	"crypto/sha1"
	"hbtrie/internal/pool"
	"testing"
)

var store *HBTrieInstance

const size = 8000

func TestInit(t *testing.T) {
	store = NewHBPlusTrie(16, pool.NewBufferpool(nil, uint64(size)))
}

func TestInsertBelowChunkSize(t *testing.T) {
	store = NewHBPlusTrie(16, pool.NewBufferpool(nil, uint64(size)))
	step := 0
	h := sha1.New()

	for i := 0; i < size; i++ {
		h.Write([]byte{byte(i)})
		key := make([]byte, 0, 16)
		key = h.Sum(nil)[:16]
		value := [8]byte{}
		copy(value[:], h.Sum(nil)[:8])

		err := store.Insert(key, value)
		if err != nil {
			t.Errorf("[step %d] while inserting to kv store(%d): %v", step, key, err)
			t.FailNow()
		}

		v, err := store.Search(key)
		if err != nil {
			t.Errorf("[step %d] while searching for key '%v': %v", step, key, err)
			t.FailNow()
		}

		if !bytes.Equal(v[:], value[:]) {
			t.Errorf("[step %d] expected %v, got %v", step, value, v)
			t.FailNow()
		}
	}
}

func TestInsertAboveChunkSize(t *testing.T) {
	store = NewHBPlusTrie(16, pool.NewBufferpool(nil, uint64(size)))
	step := 0
	h := sha1.New()

	for i := 0; i < size; i++ {
		h.Write([]byte{byte(i)})
		key := make([]byte, 0, 40)
		key = append(h.Sum(nil), h.Sum(nil)...)
		value := [8]byte{}
		copy(value[:], h.Sum(nil)[:8])

		err := store.Insert(key, value)
		if err != nil {
			t.Errorf("[step %d] while inserting to kv store(%d): %v", step, key, err)
			t.FailNow()
		}

		v, err := store.Search(key)
		if err != nil {
			t.Errorf("[step %d] while searching for key '%v': %v", step, key, err)
			t.FailNow()
		}

		if !bytes.Equal(v[:], value[:]) {
			t.Errorf("[step %d] expected %v, got %v", step, value, *v)
			t.FailNow()
		}
	}
}
