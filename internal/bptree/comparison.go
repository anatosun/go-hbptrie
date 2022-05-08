package bptree

import "bytes"

// Compare returns the comparison value between two 16 bytes arrays. The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
func Compare(a, b [16]byte) int {

	return bytes.Compare(a[:], b[:])

}

func isNull(a [16]byte) bool {
	return Compare(a, [16]byte{}) == 0
}
