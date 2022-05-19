package store

import (
	"math/rand"
	"os"
	"path"
	"testing"
)

var (
	store         Store
	array         [][]byte
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
	array = make([][]byte, 0, size)

	for i := 0; i < size; i++ {
		array = append(array, RandStringBytes(32))
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
	array = make([][]byte, 0, size)

	for i := 0; i < size; i++ {
		array = append(array, RandStringBytes(32))
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
	array = make([][]byte, 0, size)

	for i := 0; i < size; i++ {
		array = append(array, RandStringBytes(32))
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
	array = make([][]byte, 0, size)

	for i := 0; i < size; i++ {
		array = append(array, RandStringBytes(32))
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
		val := rand.Uint64()
		success, err := store.Put(array[i], val)
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

func TestGet(t *testing.T) {
	for i := 0; i < size; i++ {
		actual, err := store.Get(array[i])
		if err != nil {
			t.Fatalf("Cannot get a value from store: %v", err)
		}
		val := rand.Uint64()

		if val != actual {
			t.Fatalf("expected %v, got %v\n", array[i], actual)
		}
	}
}

func TestUpdate(t *testing.T) {

	if store.Len() == 0 {
		TestInsert(t)
	}

	for i := 0; i < len(array); i++ {
		r := RandStringBytes(8)
		val := rand.Uint64()

		success, err := store.Put(array[i], val)

		if err != nil {
			t.Errorf("error while updating %d to value %d: %v", array[i], r, err)
			t.FailNow()
		}

		if !success {
			t.Errorf("error while updating %d to value %d: value was not updated", array[i], r)
			t.FailNow()
		}
	}

	expected := len(array)
	actual := int(store.Len())

	if expected != actual {
		t.Errorf("expected %d, got %d", expected, actual)
		t.FailNow()
	}

	TestClose(t)
	TestDeleteStore(t)
}

func TestRemove(t *testing.T) {

	if store.Len() == 0 {
		TestInsert(t)
	}

	for i := 0; i < len(array); i++ {
		err := store.Delete(array[i])
		if err != nil {
			t.Errorf("while removing %v: %v", array[i], err)
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

func TestInsert2Bytes(t *testing.T) {
	store, err := NewStore(&StoreOptions{
		storePath: path.Join(os.TempDir(), "testing_2bytes_hb_store.db"),
		chunkSize: 2,
	})

	if err != nil {
		t.Fatalf("Cannot initialize store. Got %v", err)
	}

	array = make([][]byte, 0, size)

	for i := 0; i < size; i++ {
		array = append(array, RandStringBytes(32))
	}

	if store.Len() != 0 {
		t.Errorf("size should be 0 but is %d", store.Len())
		t.FailNow()
	}

	for i := 0; i < size; i++ {
		val := rand.Uint64()
		success, err := store.Put(array[i], val)
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

func TestSaveAndLoad(t *testing.T) {
	if store == nil {
		TestInit(t)
	}

	// Insert values
	TestInsert(t)
	// Close the store
	TestClose(t)
	// Open the same store again
	TestInit(t)
	// Compare the values
	TestGet(t)
}
