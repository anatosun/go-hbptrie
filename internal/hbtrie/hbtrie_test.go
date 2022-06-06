package hbtrie

import (
	"crypto/sha1"
	"crypto/sha512"
	"hbtrie/internal/pool"
	"math/rand"
	"os"
	"path"
	"testing"
)

var values map[[64]byte]uint64
var storeDataPath = path.Join(os.TempDir(), "hb_store")

const size = 10

func TestInit(t *testing.T) {

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

}

func TestInsertBelowChunkSize(t *testing.T) {
	p, err := pool.NewBufferpool(10, storeDataPath)
	if err != nil {
		t.Errorf("while creating bufferpool: %v", err)
		t.FailNow()
	}
	t.Cleanup(func() {
		p.Close()
		p.Clean()

	})
	store := NewHBPlusTrie(p)

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
	p, err := pool.NewBufferpool(10, storeDataPath)
	if err != nil {
		t.Errorf("while creating bufferpool: %v", err)
		t.FailNow()
	}
	t.Cleanup(func() {
		p.Close()
		p.Clean()

	})
	store := NewHBPlusTrie(p)

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
	p, err := pool.NewBufferpool(10, storeDataPath)
	if err != nil {
		t.Errorf("while creating bufferpool: %v", err)
		t.FailNow()
	}
	t.Cleanup(func() {
		p.Close()
		p.Clean()

	})
	store := NewHBPlusTrie(p)

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
	p, err := pool.NewBufferpool(5, storeDataPath)
	if err != nil {
		t.Errorf("while creating bufferpool: %v", err)
		t.FailNow()
	}
	t.Cleanup(func() {
		p.Close()
		p.Clean()

	})
	store := NewHBPlusTrie(p)

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

	p, err := pool.NewBufferpool(5, storeDataPath)
	if err != nil {
		t.Errorf("while creating bufferpool: %v", err)
		t.FailNow()
	}

	t.Cleanup(func() {
		p.Close()
		p.Clean()
	})

	store := NewHBPlusTrie(p)

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

}

func TestWriteAndRetrieveFromDisk(t *testing.T) {
	TestInit(t)

	p1, err := pool.NewBufferpool(5, storeDataPath)
	if err != nil {
		t.Errorf("while creating bufferpool: %v", err)
		t.FailNow()
	}

	store := NewHBPlusTrie(p1)
	step := 0
	for key, value := range values {

		err := store.Insert(key[:], value)
		if err != nil {
			t.Errorf("[step %d] while inserting to kv store(%d): %v", step, key, err)
			t.FailNow()
		}

		step++
	}

	step = 0
	for key, value := range values {

		v, err := store.Search(key[:])
		t.Logf("[step %d] searching for key '%v'", step, key)
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

	err = store.Write()
	if err != nil {
		t.Errorf("while writing to disk: %v", err)
		t.FailNow()
	}
	err = p1.Close()
	if err != nil {
		t.Errorf("while closing bufferpool: %v", err)
		t.FailNow()
	}
	p2, err := pool.NewBufferpool(5, storeDataPath)
	if err != nil {
		t.Errorf("while creating bufferpool: %v", err)
		t.FailNow()
	}

	store2, err := Read(p2)
	if err != nil {
		t.Errorf("while reading from file: %v", err)
		t.FailNow()
	}
	if store2.Len() != store.Len() {
		t.Errorf("expected %v, got %v", store.Len(), store2.Len())
		t.FailNow()
	}

	step = 0
	good := 0
	for key, value := range values {

		v, err := store2.Search(key[:])
		if err != nil {
			t.Errorf("[step %d] while searching for key '%v': %v", step, key, err)
		} else {
			good++
		}

		if v != value {
			t.Errorf("[step %d] expected %v, got %v", step, value, v)
		}

		step++
	}

	if good < len(values) {
		t.Errorf("only %d/%d were retrieved\n", good, len(values))
	}

	p2.Close()
	p1.Clean()
	p2.Clean()
}
