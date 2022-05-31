package bptree

import (
	"crypto/sha1"
	"hbtrie/internal/pool"
	"math/rand"
	"os"
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
	filename := "temp_test_data_eviction"
	t.Cleanup(func() {
		os.Remove(filename)
	})
	file, err := os.Create(filename)
	if err != nil {
		t.Errorf("could not create temp file: %v", err)
		t.FailNow()
	}
	store = NewBplusTree(pool.NewBufferpool(file, uint64(10)))
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

	err = file.Close()
	if err != nil {
		t.Errorf("could not close temp file: %v", err)
		t.FailNow()
	}

}

func TestWriteOnDisk(t *testing.T) {
	filename := "temp_test_data_write"
	t.Cleanup(func() {
		os.Remove(filename)
	})
	file, err := os.Create(filename)
	if err != nil {
		t.Errorf("could not create temp file: %v", err)
		t.FailNow()
	}
	store = NewBplusTree(pool.NewBufferpool(file, uint64(10)))
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

	file.Close()
	if err != nil {
		t.Errorf("could not close temp file: %v", err)
		t.FailNow()
	}

}

func TestCompareSimilar(t *testing.T) {
	filename1 := "temp_test_data_write1"

	file1, err := os.Create(filename1)
	if err != nil {
		t.Errorf("could not create temp file: %v", err)
		t.FailNow()
	}
	filename2 := "temp_test_data_write2"
	t.Cleanup(func() {

	})
	file2, err := os.Create(filename2)
	if err != nil {
		t.Errorf("could not create temp file: %v", err)
		t.FailNow()
	}
	t.Cleanup(func() {
		os.Remove(filename1)
		os.Remove(filename2)
	})
	store_1 := NewBplusTree(pool.NewBufferpool(file1, uint64(10)))
	store_2 := NewBplusTree(pool.NewBufferpool(file2, uint64(10)))
	step := 0
	for key, value := range values {

		success, err := store_1.Insert(key, value)
		if err != nil {
			t.Errorf("[step %d] while inserting to kv store(%d): %v", step, key, err)
			t.FailNow()
		}

		if !success {
			t.Errorf("[step %d] should be able to insert key: %v", step, key)
			t.FailNow()
		}

		v, err := store_1.Search(key)
		if err != nil {
			t.Errorf("[step %d] while searching for key '%v': %v", step, key, err)
			t.FailNow()
		}

		if v != value {
			t.Errorf("[step %d] expected %d, got %d", step, value, v)
			t.FailNow()
		}

		success, err = store_2.Insert(key, value)
		if err != nil {
			t.Errorf("[step %d] while inserting to kv store(%d): %v", step, key, err)
			t.FailNow()
		}

		if !success {
			t.Errorf("[step %d] should be able to insert key: %v", step, key)
			t.FailNow()
		}

		v, err = store_2.Search(key)
		if err != nil {
			t.Errorf("[step %d] while searching for key '%v': %v", step, key, err)
			t.FailNow()
		}

		if v != value {
			t.Errorf("[step %d] expected %d, got %d", step, value, v)
			t.FailNow()
		}

	}

	comp, res := store_1.Compare(store_2)
	if !comp {
		t.Errorf("expected same store: %s", res)
		t.FailNow()
	}
}

func TestCompareDisimilar(t *testing.T) {
	filename1 := "temp_test_data_write1"

	file1, err := os.Create(filename1)
	if err != nil {
		t.Errorf("could not create temp file: %v", err)
		t.FailNow()
	}
	filename2 := "temp_test_data_write2"
	t.Cleanup(func() {

	})
	file2, err := os.Create(filename2)
	if err != nil {
		t.Errorf("could not create temp file: %v", err)
		t.FailNow()
	}
	t.Cleanup(func() {
		os.Remove(filename1)
		os.Remove(filename2)
	})
	store_1 := NewBplusTree(pool.NewBufferpool(file1, uint64(10)))
	store_2 := NewBplusTree(pool.NewBufferpool(file2, uint64(10)))
	step := 0
	for key, value := range values {

		success, err := store_1.Insert(key, value)
		if err != nil {
			t.Errorf("[step %d] while inserting to kv store(%d): %v", step, key, err)
			t.FailNow()
		}

		if !success {
			t.Errorf("[step %d] should be able to insert key: %v", step, key)
			t.FailNow()
		}

		v, err := store_1.Search(key)
		if err != nil {
			t.Errorf("[step %d] while searching for key '%v': %v", step, key, err)
			t.FailNow()
		}

		if v != value {
			t.Errorf("[step %d] expected %d, got %d", step, value, v)
			t.FailNow()
		}

		success, err = store_2.Insert(key, value)
		if err != nil {
			t.Errorf("[step %d] while inserting to kv store(%d): %v", step, key, err)
			t.FailNow()
		}

		if !success {
			t.Errorf("[step %d] should be able to insert key: %v", step, key)
			t.FailNow()
		}

		v, err = store_2.Search(key)
		if err != nil {
			t.Errorf("[step %d] while searching for key '%v': %v", step, key, err)
			t.FailNow()
		}

		if v != value {
			t.Errorf("[step %d] expected %d, got %d", step, value, v)
			t.FailNow()
		}

	}
	for key, value := range values {
		new := value - 1
		success, err := store_2.Insert(key, new)

		if err != nil {
			t.Errorf("[step %d] while inserting to kv store(%d): %v", step, key, err)
			t.FailNow()
		}

		if success {
			t.Errorf("[step %d] should not be able to insert new key: %v", step, key)
			t.FailNow()
		}

		v, err := store_2.Search(key)
		if err != nil {
			t.Errorf("[step %d] while searching for key '%v': %v", step, key, err)
			t.FailNow()
		}

		if v != new {
			t.Errorf("[step %d] expected %d, got %d", step, new, v)
			t.FailNow()
		}

		break

	}

	comp, _ := store_1.Compare(store_2)
	if comp {
		t.Errorf("expected different store")
		t.FailNow()
	}
}
