package hbtrie

import (
	"bytes"
	"crypto/sha1"
	"hbtrie/internal/pool"
	"math/rand"
	"testing"
)

var store *HBTrieInstance

const size = 1000

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

func TestInsertSimilarAboveChunkSize(t *testing.T) {
	store = NewHBPlusTrie(16, pool.NewBufferpool(nil, uint64(size)))
	step := 0
	h := sha1.New()
	// Create 10 random prefixes
	randomPrefix := make([][16]byte, 0, 10)
	for i := 0; i < 10; i++ {
		h.Write([]byte{byte(i)})
		key := [16]byte{}
		copy(key[:], h.Sum(nil)[:16])
		randomPrefix = append(randomPrefix, key)
	}

	for i := 0; i < size; i++ {
		h.Write([]byte{byte(i)})
		key := make([]byte, 0, 40)
		// Pick randomely a prefix from a predefined list and append the key to it.
		key = append(randomPrefix[rand.Intn(10)][:], h.Sum(nil)...)
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

func TestUpdateKeys(t *testing.T) {
	store = NewHBPlusTrie(16, pool.NewBufferpool(nil, uint64(size)))
	step := 0
	h := sha1.New()

	for i := 0; i < 10; i++ {
		h.Write([]byte{byte(i)})
		key := make([]byte, 0, 40)
		// Pick randomely a prefix from a predefined list and append the key to it.
		key = append(h.Sum(nil), h.Sum(nil)...)
		value := [8]byte{}
		copy(value[:], h.Sum(nil)[:8])

		err := store.Insert(key, value)
		if err != nil {
			t.Errorf("[step %d] while inserting to kv store(%d): %v", step, key, err)
			t.FailNow()
		}

		// generate a new value
		h.Write([]byte{byte(i * 10)})
		value = [8]byte{}
		copy(value[:], h.Sum(nil)[:8])

		// Update the value with the same key
		err = store.Insert(key, value)
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
