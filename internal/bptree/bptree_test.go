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
	p, err := pool.NewBufferpool(10)
	if err != nil {
		t.Errorf("could not create bufferpool: %v", err)
		t.FailNow()
	}

	store = NewBplusTree(p)
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

		if v != value {
			t.Errorf("[step %d] expected %d, got %d", step, value, v)
			t.FailNow()
		}
		step++
	}

	expected := len(values)
	actual := int(store.Len())

	if expected != actual {
		t.Errorf("expected %d, got %d", expected, actual)
		t.FailNow()
	}

}

func TestUpdate(t *testing.T) {
	t.Cleanup(func() {
		store.pool.Close()
		store.pool.Clean()

	})
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

func TestPageEviction(t *testing.T) {

	p, err := pool.NewBufferpool(10)
	if err != nil {
		t.Errorf("could not create bufferpool: %v", err)
		t.FailNow()
	}
	t.Cleanup(func() {
		p.Close()
		p.Clean()

	})
	store = NewBplusTree(p)
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

		if v != value {
			t.Errorf("[step %d] expected %d, got %d", step, value, v)
			t.FailNow()
		}
		step++
	}

}

func TestWriteOnDisk(t *testing.T) {

	p, err := pool.NewBufferpool(10)
	if err != nil {
		t.Errorf("could not create bufferpool: %v", err)
		t.FailNow()
	}
	t.Cleanup(func() {
		p.Close()
		p.Clean()

	})
	store = NewBplusTree(p)
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

		if v != value {
			t.Errorf("[step %d] expected %d, got %d", step, value, v)
			t.FailNow()
		}
		step++
	}
	err = store.Write()
	if err != nil {
		t.Errorf("could not write to disk: %v", err)
		t.FailNow()
	}

}

func TestWriteAndRetrieveFromDisk(t *testing.T) {

	p, err := pool.NewBufferpool(5)
	if err != nil {
		t.Errorf("could not create bufferpool: %v", err)
		t.FailNow()
	}
	t.Cleanup(func() {
		p.Close()
		p.Clean()

	})
	store = NewBplusTree(p)
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

		if v != value {
			t.Errorf("[step %d] expected %d, got %d", step, value, v)
			t.FailNow()
		}
		step++
	}
	frame := store.frameId
	err = store.Write()
	if err != nil {
		t.Errorf("could not write to disk: %v", err)
		t.FailNow()
	}

	p, err = pool.NewBufferpool(5)
	if err != nil {
		t.Errorf("could not create bufferpool: %v", err)
		t.FailNow()
	}
	store2, err := ReadBpTreeFromDisk(p, frame)
	if err != nil {
		t.Errorf("could not read from disk: %v", err)
		t.FailNow()
	}
	step = 0
	for key, value := range values {

		v, err := store2.Search(key)
		if err != nil {
			t.Errorf("[step %d] while searching for key '%v': %v", step, key, err)
			t.FailNow()
		}

		if v != value {
			t.Errorf("[step %d] expected %d, got %d", step, value, v)
			t.FailNow()
		}
		step++
	}

}

func TestWriteAndRetrieveFromDiskMultiple(t *testing.T) {

	pl1, err := pool.NewBufferpool(uint64(5))
	if err != nil {
		t.Errorf("could not create bufferpool: %v", err)
		t.FailNow()
	}
	stores := make([]*BPlusTree, size)
	t.Cleanup(func() {
		pl1.Close()
		pl1.Clean()

	})
	for i := 0; i < size; i++ {
		stores[i] = NewBplusTree(pl1)
	}
	values = make(map[[16]byte]uint64)
	h := sha1.New()

	for i := 0; i < 100; i++ {
		h.Write([]byte{byte(i)})
		key := [16]byte{}
		copy(key[:], h.Sum(nil)[:16])
		value := rand.Uint64()
		values[key] = value
	}

	for key, value := range values {
		step := 0

		for _, store := range stores {
			if store.frameId != uint64(step+1) {
				t.Errorf("[step %d] expected frame id %d, got %d", step, step+1, store.frameId)
				t.FailNow()
			}
			success, err := stores[step].Insert(key, value)
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

			if v != value {
				t.Errorf("[step %d] expected %d, got %d", step, value, v)
				t.FailNow()
			}
			step++
		}

	}

	for _, store := range stores {
		err = store.Write()
		if err != nil {
			t.Errorf("could not write to disk: %v", err)
			t.FailNow()
		}
	}

	pl2, err := pool.NewBufferpool(uint64(5))
	if err != nil {
		t.Errorf("could not create bufferpool: %v", err)
		t.FailNow()
	}

	stores2 := make([]*BPlusTree, size)
	for frame := uint64(1); frame < size+1; frame++ {
		s, err := ReadBpTreeFromDisk(pl2, frame)
		if err != nil {
			t.Errorf("could not read from disk: %v", err)
			t.FailNow()
		}
		stores2[frame-1] = s

	}

	t.Cleanup(func() {
		pl1.Close()
		pl1.Clean()
		pl2.Close()
		pl2.Clean()
	})

	for key, value := range values {
		step := 0

		for _, store := range stores2 {
			v, err := store.Search(key)
			if err != nil {
				t.Errorf("[step %d] while searching for key '%v': %v", step, key, err)
				t.FailNow()
			}

			if v != value {
				t.Errorf("[step %d] expected %d, got %d", step, value, v)
				t.FailNow()
			}
			step++
		}
	}
}
