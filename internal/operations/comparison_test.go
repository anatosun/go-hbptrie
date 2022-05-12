package operations

import (
	"testing"
)

func TestStringComparison(t *testing.T) {

	a := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	b := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	if Compare(a, b) != 0 {
		t.Errorf("expected %d, got %d", 0, Compare(a, b))
		t.FailNow()
	}

	if !Equal(a, b) {
		t.Errorf("expected %t, got %t", true, Equal(a, b))
		t.FailNow()
	}

	a = [16]byte{1, 2, 3, 4, 5, 255, 7, 8, 9, 10, 11, 12, 13, 14, 15, 15}
	b = [16]byte{1, 2, 3, 4, 5, 255, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

	if Compare(a, b) != -1 {
		t.Errorf("expected %d, got %d", -1, Compare(a, b))
		t.FailNow()
	}

	a = [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 255, 14, 15, 17}
	b = [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 255, 14, 15, 16}

	if Compare(a, b) != 1 {
		t.Errorf("expected %d, got %d", 1, Compare(a, b))
		t.FailNow()
	}

	a = [16]byte{}

	if !IsNull(a) {
		t.Errorf("expected %t, got %t", true, IsNull(a))
		t.FailNow()
	}

	b = [16]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

	if !IsNull(b) {
		t.Errorf("expected %t, got %t", true, IsNull(b))
		t.FailNow()
	}
}
