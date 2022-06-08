package writebufferindex

import (
	"hbtrie/internal/hbtrie"
	"hbtrie/internal/kverrors"
)

type WriteBufferIndex struct {
	index map[string]uint64 // Hashtable
	hbt   *hbtrie.HBTrieInstance
}

func NewWriteBufferIndex(hbt *hbtrie.HBTrieInstance) *WriteBufferIndex {
	return &WriteBufferIndex{index: make(map[string]uint64), hbt: hbt}
}

// Inserts a key to the hashtable.
func (wb *WriteBufferIndex) Insert(key []byte, value uint64) {
	// Convert key from byte slice to string
	wb.index[string(key)] = value
}

// Searches a key in the hashtable and returns the value
func (wb *WriteBufferIndex) Search(key []byte) (uint64, error) {
	// Search key in hashtable
	if val, found := wb.index[string(key)]; found {
		return val, nil
	}

	return 0, &kverrors.KeyNotFoundError{Key: key}
}

// Inserts all entries from hashtable to hbtrie. After a successfull insertion, the entry is removed from the hashtable.
func (wb *WriteBufferIndex) Flush() error {
	var key []byte
	errFlushFailed := &kverrors.PartialWriteError{Total: len(wb.index)}
	success := 0
	isWriteError := false
	for keyString, value := range wb.index {
		// Convert key from string to byte slice again.
		key = []byte(keyString)
		err := wb.hbt.Insert(key, value)
		// Insert has succeed
		// Delete entry from hashmap
		if err == nil {
			success++
			delete(wb.index, keyString)
		} else {
			isWriteError = true
		}
	}
	// Add amount of successful inserts
	errFlushFailed.Written = success

	// Return error in case a insertion was not successful
	if isWriteError {
		return errFlushFailed
	}

	return nil
}
