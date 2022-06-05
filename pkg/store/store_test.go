package store

import (
	"crypto/sha512"
	"math/rand"
	"os"
	"path"
	"testing"
)

var (
	store         Store
	values        map[[256]byte]uint64
	testStorePath = path.Join(os.TempDir(), "testing_hb_store")
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
	values = make(map[[256]byte]uint64)

	h := sha512.New()

	for i := 0; i < size; i++ {
		h.Write([]byte{byte(i)})
		key := [256]byte{}
		copy(key[:], h.Sum(nil)[:])
		value := rand.Uint64()
		values[key] = value
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
	store = nil
}

func TestInitWithoutPath(t *testing.T) {
	var err error
	store, err = NewStore(&StoreOptions{chunkSize: 8})
	if err != nil {
		t.Fatalf("Cannot initialize store. Got %v", err)
	}
	values = make(map[[256]byte]uint64)

	h := sha512.New()

	for i := 0; i < size; i++ {
		h.Write([]byte{byte(i)})
		key := [256]byte{}
		copy(key[:], h.Sum(nil)[:])
		value := rand.Uint64()
		values[key] = value
	}

	if store.Len() != 0 {
		t.Errorf("size should be 0 but is %d", store.Len())
		t.FailNow()
	}

	TestClose(t)
	TestDeleteStore(t)
}

func TestInitWithoutChunkSize(t *testing.T) {
	var err error
	store, err = NewStore(&StoreOptions{storePath: testStorePath})
	if err != nil {
		t.Fatalf("Cannot initialize store. Got %v", err)
	}
	values = make(map[[256]byte]uint64)

	h := sha512.New()

	for i := 0; i < size; i++ {
		h.Write([]byte{byte(i)})
		key := [256]byte{}
		copy(key[:], h.Sum(nil)[:])
		value := rand.Uint64()
		values[key] = value
	}

	if store.Len() != 0 {
		t.Errorf("size should be 0 but is %d", store.Len())
		t.FailNow()
	}
	TestClose(t)
	TestDeleteStore(t)
}

func TestInitWithDefault(t *testing.T) {
	var err error
	store, err = NewStore(&StoreOptions{})
	if err != nil {
		t.Fatalf("Cannot initialize store. Got %v", err)
	}
	values = make(map[[256]byte]uint64)

	h := sha512.New()

	for i := 0; i < size; i++ {
		h.Write([]byte{byte(i)})
		key := [256]byte{}
		copy(key[:], h.Sum(nil)[:])
		value := rand.Uint64()
		values[key] = value
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

	for k, v := range values {
		success, err := store.Put(k[:], v)
		if err != nil || !success {
			t.Errorf("while inserting to kv store(%d): %v", k, err)
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

func TestGet(t *testing.T) {
	for k, v := range values {
		actual, err := store.Get(k[:])
		if err != nil {
			t.Fatalf("Cannot get a value from store: %v", err)
		}

		if v != actual {
			t.Fatalf("expected %v, got %v\n", v, actual)
		}
	}
}

func TestUpdate(t *testing.T) {

	if store.Len() == 0 {
		TestInsert(t)
	}

	for k, v := range values {
		r := rand.Uint64()
		if r != v {
			success, err := store.Put(k[:], r)

			if err != nil {
				t.Errorf("error while updating %d to value %d: %v", k, r, err)
				t.FailNow()
			}

			if !success {
				t.Errorf("error while updating %d to value %d: value was not updated", k, r)
				t.FailNow()
			}
		}
	}

	expected := len(values)
	actual := int(store.Len())

	if expected != actual {
		t.Errorf("expected %d, got %d", expected, actual)
		t.FailNow()
	}

	TestClose(t)
	TestDeleteStore(t)
}

// func TestRemove(t *testing.T) {

// 	if store.Len() == 0 {
// 		TestInsert(t)
// 	}

// 	for i := 0; i < len(values); i++ {
// 		err := store.Delete(values[i])
// 		if err != nil {
// 			t.Errorf("while removing %v: %v", values[i], err)
// 			t.FailNow()
// 		}

// 	}

// 	expected := 0
// 	actual := int(store.Len())

// 	if expected != actual {
// 		t.Errorf("expected %d, got %d", expected, actual)
// 		t.FailNow()
// 	}
// }

func TestInsert2Bytes(t *testing.T) {
	store, err := NewStore(&StoreOptions{
		storePath: path.Join(os.TempDir(), "testing_2bytes_hb_store"),
		chunkSize: 2,
	})

	if err != nil {
		t.Fatalf("Cannot initialize store. Got %v", err)
	}

	values = make(map[[256]byte]uint64)

	h := sha512.New()

	for i := 0; i < size; i++ {
		h.Write([]byte{byte(i)})
		key := [256]byte{}
		copy(key[:], h.Sum(nil)[:2])
		value := rand.Uint64()
		values[key] = value
	}

	for k, v := range values {
		success, err := store.Put(k[:], v)
		if err != nil || !success {
			t.Errorf("while inserting to kv store(%d): %v", k, err)
			t.FailNow()
		}
	}

	expected := len(values)
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
