package state

import (
	"testing"
)

func TestCounter(t *testing.T) {
	counter := NewCounter()
	(*counter).Increment()
	if (*counter).Counter != 1 {
		t.Error("Counter not incremented")
	}
}
