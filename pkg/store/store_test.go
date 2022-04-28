package store

import (
	"math/rand"
	"testing"
	"unsafe"
)

var store Store
var array []int

const size = 1000

func TestInit(t *testing.T) {
	// store = New(3)
	array = make([]int, 0, size)

	for i := 0; i < size; i++ {
		array = append(array, i)
	}

	if store.Len() != 0 {
		t.Errorf("size should be 0 but is %d", store.Len())
		t.FailNow()
	}

}

func TestInsert(t *testing.T) {
	//	t.Logf("inserting %d random keys", size)

	for i := 0; i < size; i++ {
		b := make([]byte, unsafe.Sizeof(array[i]))
		success, err := store.Put(b, b)
		if err != nil || !success {
			t.Errorf("while inserting to kv store(%d): %v", i, err)
			t.FailNow()
		}
	}

	expected := len(array)
	actual := int(store.Len())

	if expected != actual {
		t.Errorf("expected %d, got %d", expected, actual)
		t.FailNow()
	}
}

func TestRemove(t *testing.T) {

	if store.Len() == 0 {
		TestInsert(t)
	}

	for i := 0; i < len(array); i++ {
		b := make([]byte, unsafe.Sizeof(array[i]))
		err := store.Delete(b)
		if err != nil {
			t.Errorf("while removing %d: %v", array[i], err)
			t.FailNow()
		}

	}

	expected := 0
	actual := int(store.Len())

	if expected != actual {
		t.Errorf("expected %d, got %d", expected, actual)
		t.FailNow()
	}
}

func TestUpdate(t *testing.T) {

	if store.Len() == 0 {
		TestInsert(t)
	}

	for i := 0; i < len(array); i++ {
		r := rand.Int()
		if r != array[i] {
			b := make([]byte, unsafe.Sizeof(array[i]))
			c := make([]byte, unsafe.Sizeof(r))
			success, err := store.Put(b, c)

			if err != nil {
				t.Errorf("error while updating %d to value %d: %v", array[i], r, err)
				t.FailNow()
			}

			if success {
				t.Errorf("error while updating %d to value %d: value was not updated", array[i], r)
				t.FailNow()
			}
		}
	}

	expected := len(array)
	actual := int(store.Len())

	if expected != actual {
		t.Errorf("expected %d, got %d", expected, actual)
		t.FailNow()
	}
}
