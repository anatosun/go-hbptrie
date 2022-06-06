package hbtrie

import (
	"errors"
	"hbtrie/internal/bptree"
	"hbtrie/internal/kverrors"
	"hbtrie/internal/pool"
)

type HBTrieInstance struct {
	rootTree  *bptree.BPlusTree // Pointer to Root B+ tree
	pool      *pool.Bufferpool
	chunkSize int // default 16 bytes
	size      uint64
}

func NewHBPlusTrie(pool *pool.Bufferpool) *HBTrieInstance {
	tree := bptree.NewBplusTree(pool)

	return &HBTrieInstance{
		pool:      pool,
		chunkSize: 16,
		rootTree:  tree,
	}
}

func (hbt *HBTrieInstance) Search(key []byte) (uint64, error) {
	// Search in the Root tree for the chunked key
	val, _, _, err := hbt.search(hbt.rootTree, key)
	if err != nil {
		return 0, err
	}

	return val, nil
}

// search recursively search for a key in the node and its children.
func (hbt *HBTrieInstance) search(bpt *bptree.BPlusTree, key []byte) (uint64, []byte, *bptree.BPlusTree, error) {
	chunkedKey, trimmedKey := createChunkFromKey(key)
	// Search in the Root tree for the chunked key
	val, err := bpt.SearchTreeEntry(*chunkedKey)
	if err != nil {
		return 0, key, bpt, err
	}

	// Check if the leaf node is a pointer to a subtree.
	if val.IsTree {
		// Decode the frameId from the value field
		// Load b+ tree instance using the frameid
		subbpt := bptree.LoadBplusTree(hbt.pool, val.Value)
		// Call recursively search.
		return hbt.search(subbpt, *trimmedKey)
	} else {
		// it is a leaf entry
		return val.Value, key, bpt, nil
	}
}

func (hbt *HBTrieInstance) Insert(key []byte, value uint64) (err error) {
	errKeyNotFound := &kverrors.KeyNotFoundError{Key: key}

	_, trimmedKey, bpt, err := hbt.search(hbt.rootTree, key)
	if err != nil {
		// Key doesn't exist
		if errors.As(err, &errKeyNotFound) {
			hbt.size++
			return hbt.insert(trimmedKey, value, key, bpt)
		} else {
			// Unknown error
			return err
		}

	}
	// If key exists, then update the value
	// We have the reference to the last subtree and the remaining key.
	err = hbt.insert(trimmedKey, value, key, bpt)

	return err

}

func (hbt *HBTrieInstance) Len() uint64 {
	return hbt.size
}

func (hbt *HBTrieInstance) insert(key []byte, value uint64, fullKey []byte, bpt *bptree.BPlusTree) error {
	chunkedKey, trimmedKey := createChunkFromKey(key)
	// If key is longer than 16 bytes
	if len(key) > 16 {
		subTree, err := hbt.createSubTree(bpt, *chunkedKey)
		if err != nil {
			return err
		}
		// Create recursively a new b+ tree instance
		return hbt.insert(*trimmedKey, value, fullKey, subTree)
	} else {
		// Key is smaller than 16 bytes => create a leaf node.
		success, err := bpt.InsertWithFullKey(*chunkedKey, value, fullKey)
		if success {
			return nil
		}

		return err
	}

}

func (hbt *HBTrieInstance) createSubTree(bpt *bptree.BPlusTree, key [16]byte) (*bptree.BPlusTree, error) {
	subTree := bptree.NewBplusTree(hbt.pool)

	treeFrameId := subTree.GetFrameId()
	success, err := bpt.InsertSubTree(key, treeFrameId)

	if success {
		return subTree, nil
	}

	return subTree, err
}

func createChunkFromKey(key []byte) (*[16]byte, *[]byte) {
	chunkedKey := [16]byte{}
	var trimmedKey []byte
	if len(key) > 16 {
		trimmedKey = make([]byte, 0, len(key)-16)
		// Chunked key of 16 bytes
		copy(chunkedKey[:], key[:16])
		// original key removed prefix
		trimmedKey = key[16:]
	} else {
		trimmedKey = make([]byte, 0, len(key))
		copy(chunkedKey[:], key[:])
		trimmedKey = key
	}
	return &chunkedKey, &trimmedKey
}

func (hbt *HBTrieInstance) Write() error {

	return hbt.pool.WriteTrie(hbt.rootTree.GetFrameId(), hbt.size)
}

func Read(pool *pool.Bufferpool) (*HBTrieInstance, error) {
	trie := &HBTrieInstance{}
	trie.pool = pool
	rootId, size, nframes, err := pool.ReadTrie()
	if err != nil {
		return trie, err
	}

	trie.size = size
	trie.chunkSize = 16
	root := bptree.LoadBplusTree(pool, rootId)
	trie.rootTree = root

	for i := uint64(1); i < nframes; i++ {
		bptree.LoadBplusTree(pool, i)

	}
	return trie, nil
}
