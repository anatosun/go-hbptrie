package store

// keys and values are byte arrays for now but may be changed in the future

// Store is an interface for a key-value store and follows the
// Create, Read, Update, Delete (CRUD) operations
type Store interface {

	// Get returns the value for the given key.
	Get(key []byte) (value []byte, err error)

	// Set sets the value for the given key.
	Put(key []byte, value []byte) (err error)

	// Delete deletes the value for the given key.
	Delete(key []byte) (err error)
}
