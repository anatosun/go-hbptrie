package store

import (
	"math/rand"
	"os"
	"path"
	"testing"
	"unsafe"
)

var (
	store         Store
	array         []int
	testStorePath = path.Join(os.TempDir(), "testing_hb_store.db")
)

const (
	size        = 1000
	letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

// Random string generator
// Returns a byte slice with size n containing random alpha characters.
func RandStringBytes(n int) []byte {
	b := make([]byte, 0, n)
	for i := 0; i < n; i++ {
		b = append(b, letterBytes[rand.Intn(len(letterBytes))])
	}
	return b
}

func TestInit(t *testing.T) {
	var err error
	store, err = NewStore(&StoreOptions{storePath: testStorePath, chunkSize: 8})
	if err != nil {
		t.Fatalf("Cannot initialize store. Got %v", err)
	}
	array = make([]int, 0, size)

	for i := 0; i < size; i++ {
		array = append(array, i)
	}

	if store.Len() != 0 {
		t.Errorf("size should be 0 but is %d", store.Len())
		t.FailNow()
	}

}

func TestClose(t *testing.T) {
	err := store.Close()
	if err != nil {
		t.Fatalf("Cannot close the store %v", err)
	}
}

func TestDeleteStore(t *testing.T) {
	err := store.DeleteStore()
	if err != nil {
		t.Fatalf("Cannot delete the store %v", err)
	}
}

func TestInitWithoutPath(t *testing.T) {
	store, err := NewStore(&StoreOptions{chunkSize: 8})
	if err != nil {
		t.Fatalf("Cannot initialize store. Got %v", err)
	}
	array = make([]int, 0, size)

	for i := 0; i < size; i++ {
		array = append(array, i)
	}

	if store.Len() != 0 {
		t.Errorf("size should be 0 but is %d", store.Len())
		t.FailNow()
	}

	TestClose(t)
	TestDeleteStore(t)
}

func TestInitWithoutChunkSize(t *testing.T) {
	store, err := NewStore(&StoreOptions{storePath: testStorePath})
	if err != nil {
		t.Fatalf("Cannot initialize store. Got %v", err)
	}
	array = make([]int, 0, size)

	for i := 0; i < size; i++ {
		array = append(array, i)
	}

	if store.Len() != 0 {
		t.Errorf("size should be 0 but is %d", store.Len())
		t.FailNow()
	}
	TestClose(t)
	TestDeleteStore(t)
}

func TestInitWithDefault(t *testing.T) {
	store, err := NewStore(&StoreOptions{})
	if err != nil {
		t.Fatalf("Cannot initialize store. Got %v", err)
	}
	array = make([]int, 0, size)

	for i := 0; i < size; i++ {
		array = append(array, i)
	}

	if store.Len() != 0 {
		t.Errorf("size should be 0 but is %d", store.Len())
		t.FailNow()
	}
	TestClose(t)
	TestDeleteStore(t)
}

func TestInsert(t *testing.T) {
	if store == nil {
		TestInit(t)
	}
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

func TestInsert2Bytes(t *testing.T) {
	store, err := NewStore(&StoreOptions{
		storePath: path.Join(os.TempDir(), "testing_2bytes_hb_store.db"),
		chunkSize: 2,
	})

	if err != nil {
		t.Fatalf("Cannot initialize store. Got %v", err)
	}

	array = make([]int, 0, size)

	for i := 0; i < size; i++ {
		array = append(array, i)
	}

	if store.Len() != 0 {
		t.Errorf("size should be 0 but is %d", store.Len())
		t.FailNow()
	}

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

	err = store.Close()
	if err != nil {
		t.Fatalf("Cannot close the store: Error %v", err)
	}

	err = store.DeleteStore()
	if err != nil {
		t.Fatalf("Cannot close the store: Error %v", err)
	}
}
