package store

// keys and values are byte arrays for now but may be changed in the future

// Store is an interface for a key-value store and follows the
// Create, Read, Update, Delete (CRUD) operations
type Store interface {

	// Get returns the value for the given key.
	Get(key []byte) (value []byte, err error)

	// Set sets the value for the given key
	// When error is nil outputs true in the case of a successful insertion
	// and false in the case of an update
	Put(key []byte, value []byte) (inserted bool, err error)

	// Delete deletes the value for the given key.
	Delete(key []byte) (err error)

	// Len returns the number of items in the store.
	Len() uint64
}
