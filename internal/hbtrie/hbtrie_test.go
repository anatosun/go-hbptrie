package hbtrie

import (
	"crypto/sha1"
	"crypto/sha512"
	"hbtrie/internal/pool"
	"math/rand"
	"os"
	"testing"
)

var store *HBTrieInstance
var values map[[64]byte]uint64

const size = 1000

func TestInit(t *testing.T) {
	store = NewHBPlusTrie(16, pool.NewBufferpool(nil, uint64(size)))
	values = make(map[[64]byte]uint64)
	h := sha512.New()

	for i := 0; i < size; i++ {
		h.Write([]byte{byte(i)})
		key := [64]byte{}
		copy(key[:32], h.Sum(nil)[:32])
		copy(key[32:64], h.Sum(nil)[:32])
		value := rand.Uint64()
		values[key] = value
	}

	if store.Len() != 0 {
		t.Errorf("expected size %d, got %d", 0, store.Len())
		t.FailNow()
	}
}

func TestInsertBelowChunkSize(t *testing.T) {
	store = NewHBPlusTrie(16, pool.NewBufferpool(nil, uint64(size)))
	step := 0

	for key, value := range values {
		k := key[:8]
		err := store.Insert(k, value)
		if err != nil {
			t.Errorf("[step %d] while inserting to kv store(%d): %v", step, key, err)
			t.FailNow()
		}

		v, err := store.Search(k)
		if err != nil {
			t.Errorf("[step %d] while searching for key '%v': %v", step, key, err)
			t.FailNow()
		}

		if v != value {
			t.Errorf("[step %d] expected %v, got %v", step, value, v)
			t.FailNow()
		}
	}
}

func TestInsertAboveChunkSize(t *testing.T) {
	store = NewHBPlusTrie(16, pool.NewBufferpool(nil, uint64(size)))
	step := 0

	for key, value := range values {

		err := store.Insert(key[:], value)
		if err != nil {
			t.Errorf("[step %d] while inserting to kv store(%d): %v", step, key, err)
			t.FailNow()
		}

		v, err := store.Search(key[:])
		if err != nil {
			t.Errorf("[step %d] while searching for key '%v': %v", step, key, err)
			t.FailNow()
		}

		if v != value {
			t.Errorf("[step %d] expected %v, got %v", step, value, v)
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
		// key := make([]byte, 0, 40)
		// Pick randomely a prefix from a predefined list and append the key to it.
		key := append(randomPrefix[rand.Intn(10)][:], h.Sum(nil)...)
		value := rand.Uint64()

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

		if v != value {
			t.Errorf("[step %d] expected %v, got %v", step, value, v)
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
		// key := make([]byte, 0, 40)
		// Pick randomely a prefix from a predefined list and append the key to it.
		key := append(h.Sum(nil), h.Sum(nil)...)
		value := rand.Uint64()

		err := store.Insert(key, value)
		if err != nil {
			t.Errorf("[step %d] while inserting to kv store(%d): %v", step, key, err)
			t.FailNow()
		}

		// generate a new value
		h.Write([]byte{byte(i * 10)})
		value = rand.Uint64()

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

		if v != value {
			t.Errorf("[step %d] expected %v, got %v", step, value, v)
			t.FailNow()
		}
	}
}

func TestInsertWithPageEviction(t *testing.T) {
	filename := "test_insert_with_page_eviction"
	file, err := os.Create(filename)
	if err != nil {
		t.Errorf("while creating file '%v': %v", filename, err)
		t.FailNow()
	}
	t.Cleanup(func() {
		file.Close()
		os.Remove(filename)
	})
	store = NewHBPlusTrie(16, pool.NewBufferpool(file, uint64(5)))

	step := 0
	for key, value := range values {

		err := store.Insert(key[:], value)
		if err != nil {
			t.Errorf("[step %d] while inserting to kv store(%d): %v", step, key, err)
			t.FailNow()
		}

		v, err := store.Search(key[:])
		if err != nil {
			t.Errorf("[step %d] while searching for key '%v': %v", step, key, err)
			t.FailNow()
		}

		if v != value {
			t.Errorf("[step %d] expected %v, got %v", step, value, v)
			t.FailNow()
		}

		step++
	}
	stat, err := os.Stat(filename)
	if err != nil {
		t.Errorf("while getting file stats: %v", err)
		t.FailNow()

	}
	if stat.Size() == 0 {
		t.Errorf("file size is 0")
		t.FailNow()
	}
}
