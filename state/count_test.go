package state

import (
	"testing"
)

func TestCounter(t *testing.T) {
	counter := NewCounter()
	(*counter).Increment()
	if (*counter).Count != 1 {
		t.Error("Counter not incremented")
	}
}

func TestCounterWindowing(t *testing.T) {
	counter := NewCounter()
	(*counter).Increment()
	(*counter).Increment()
	(*counter).Increment()
	if (*counter).Window() != 3 {
		t.Error("Counter not incremented")
	}
	if (*counter).Count != 0 {
		t.Error("Expected counter to be zero")
	}
}
