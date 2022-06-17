# Go HB+Trie

## Overview

This is a simple Go implementation of a Hierarchical B+ Trie (HB+ Trie) originally described in [this paper](https://www.computer.org/csdl/journal/tc/2016/03/07110563/13rRUyuegos) and mainly addressed as [ForestDB](https://github.com/couchbase/forestdb).

## Structure 

### `internal` folder

The core implementation of the Trie can be found in the `internal` folder which has the self-explanatory structure outlined below.
```
├── bptree
│   ├── bptree.go
│   ├── bptree_test.go
│   └── memory.go
├── hbtrie
│   ├── hbtrie.go
│   └── hbtrie_test.go
├── kverrors
│   └── errors.go
├── operations
│   ├── comparison.go
│   └── comparison_test.go
├── pool
│   ├── entry.go
│   ├── entry_test.go
│   ├── frame.go
│   ├── leaf.go
│   ├── leaf_test.go
│   ├── metadata.go
│   ├── metadata_test.go
│   ├── node.go
│   ├── node_test.go
│   ├── page.go
│   └── pool.go
├── README.md
└── writebufferindex
    └── writebufferindex.go
```

The bufferpool (i.e., `pool`) is at the kernel of this implementation and is the memory orchestrator of the multiple B+ trees and thus the HB+ Trie. The concept is the following. The bufferpool consists of frames that handle the memory in a LRU fashion for each B+ Tree. Upon initialisation, a tree registers to the pool and is given a frame id that it should provide for each subsequent query (fetching the memory reference or for allocating new nodes).

### `pkg` folder

The functions that can be used as an external package are all included in the `pkg` folder. A little sample on how to use the HB+ Trie can be found below. This example insert a 256 bytes keys whereas the chunk size is of 16 bytes. The key is formed with 8 concatenations of the same `sha512` value.

```Go
	store, err := hbtrie.NewStore(&StoreOptions{chunkSize: 8})
	if err != nil {
		return err
	}
	

	h := sha512.New()

	for i := 0; i < size; i++ {
		h.Write([]byte{byte(i)})
		key := [256]byte{}
		copy(key[:32], h.Sum(nil)[:])
		copy(key[32:64], h.Sum(nil)[:])
		copy(key[64:96], h.Sum(nil)[:])
		copy(key[96:128], h.Sum(nil)[:])
		copy(key[128:160], h.Sum(nil)[:])
		copy(key[160:192], h.Sum(nil)[:])
		copy(key[192:], h.Sum(nil)[:])
		value := rand.Uint64()
		err = store.Put(key[:], value)

        if err != nil {
            return err
        }
	}
```

All in all, the HB+ Trie implements the following interface and is accessible via the same functions.

```go
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
```


## Testing

To run the tests, execute in the home directory of the project `go test -v ./...`.
