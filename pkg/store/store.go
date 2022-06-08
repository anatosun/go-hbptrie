package store

import (
	"errors"
	"hbtrie/internal/hbtrie"
	"hbtrie/internal/kverrors"
	"hbtrie/internal/pool"
	"hbtrie/internal/writebufferindex"
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

	// Flushes the Write buffer index. Inserts all entries from write buffer to hbtrie
	FlushWriteBuffer() error

	// Flushes Write Buffer and then writes entries from hbtrie to disk.
	Flush() error

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
	storePath   string
	chunkSize   int
	pool        *pool.Bufferpool
	hbtrie      *hbtrie.HBTrieInstance
	writeBuffer *writebufferindex.WriteBufferIndex
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
		options.storePath = path.Join(os.TempDir(), "hb_store")
	}

	p, err := pool.NewBufferpool(uint64(bufferpoolSize), options.storePath)
	if err != nil {
		return nil, err
	}

	hbt := hbtrie.NewHBPlusTrie(p)
	wb := writebufferindex.NewWriteBufferIndex(hbt)

	return &HBTrieStore{
		storePath:   options.storePath,
		chunkSize:   options.chunkSize,
		pool:        p,
		hbtrie:      hbt,
		writeBuffer: wb,
	}, nil
}

func (s *HBTrieStore) Close() error {
	return s.pool.Close()
}

func (s *HBTrieStore) DeleteStore() error {
	return s.pool.Clean()
}

func (s *HBTrieStore) Get(key []byte) (value uint64, err error) {
	var keyError *kverrors.KeyNotFoundError
	val, err := s.writeBuffer.Search(key)
	// If key has been found in write buffer index, then return val
	if err == nil {
		return val, nil
	}

	// If key has not been found, then search in hbtrie
	if errors.As(err, &keyError) {
		val, err = s.hbtrie.Search(key)
		return val, err
	}

	// if writebuffer returned an unknown error, then throw error
	return 0, err
}

func (s *HBTrieStore) Put(key []byte, value uint64) (inserted bool, err error) {
	// Insert entry to write buffer only
	s.writeBuffer.Insert(key, value)

	return true, nil
}

func (s *HBTrieStore) FlushWriteBuffer() error {
	return s.writeBuffer.Flush()
}

func (s *HBTrieStore) Flush() error {
	err := s.FlushWriteBuffer()
	if err != nil {
		return err
	}
	err = s.hbtrie.Write()

	return err
}

func (s *HBTrieStore) Len() uint64 {
	return s.hbtrie.Len()
}
