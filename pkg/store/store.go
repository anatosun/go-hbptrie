package store

import (
	"hbtrie/internal/hbtrie"
	"hbtrie/internal/pool"
	"os"
	"path"
)

type StoreManager interface {
	// Creates or opens a store.
	// Store will be nil in case of an error.
	NewStore(*StoreOptions) (Store, error)
}

// keys and values are byte arrays for now but may be changed in the future

// Store is an interface for a key-value store and follows the
// Create, Read, Update, Delete (CRUD) operations
type Store interface {

	// Closes the store. Changes are commited to disk and file handles is closed.
	Close() (err error)

	DeleteStore() (err error)

	// Get returns the value for the given key.
	Get(key []byte) (value uint64, err error)

	// Set sets the value for the given key
	// When error is nil outputs true in the case of a successful insertion
	// and false in the case of an update
	Put(key []byte, value uint64) (inserted bool, err error)

	// Delete deletes the value for the given key.
	Delete(key []byte) (err error)

	// Len returns the number of items in the store.
	Len() uint64
}

// Options struct used to create a new store.
type StoreOptions struct {
	// file path of the store.
	storePath string
	// Configurable chunk size in bytes for HB+ trie
	// Default 8 bytes
	chunkSize int
}

type HBTrieStore struct {
	storePath string
	chunkSize int
	pool      *pool.Bufferpool
	hbtrie    *hbtrie.HBTrieInstance
}

const (
	bufferpoolSize = 8000
)

func NewStore(options *StoreOptions) (Store, error) {
	// Chunk size is not set, then default 8 bytes
	if options.chunkSize == 0 {
		options.chunkSize = 16
	}

	if len(options.storePath) == 0 {
		options.storePath = path.Join(os.TempDir(), "hb_store.db")
	}

	p, err := pool.NewBufferpool(uint64(bufferpoolSize))
	if err != nil {
		return nil, err
	}

	return &HBTrieStore{
		storePath: options.storePath,
		chunkSize: options.chunkSize,
		pool:      p,
		hbtrie:    hbtrie.NewHBPlusTrie(p),
	}, nil
}

func (s *HBTrieStore) Close() error {
	// TODO: Implement
	return nil
}

func (s *HBTrieStore) DeleteStore() error {
	// TODO: Implement
	return nil
}

func (s *HBTrieStore) Get(key []byte) (value uint64, err error) {
	val, err := s.hbtrie.Search(key)
	return val, err
}

func (s *HBTrieStore) Put(key []byte, value uint64) (inserted bool, err error) {
	err = s.hbtrie.Insert(key, value)

	if err != nil {
		return false, err
	}

	return true, nil
}

func (s *HBTrieStore) Delete(key []byte) (err error) {
	panic("Not implemented")
}

func (s *HBTrieStore) Len() uint64 {
	return s.hbtrie.Len()
}
