package main

import (
	"testing"
	"time"

	"github.com/patrobinson/go-fish/output"
	"github.com/patrobinson/go-fish/state"
)

type TestRule struct {
	windowCounter int
}

func (r *TestRule) Init(state.State) error {
	return nil
}
func (r *TestRule) Close() {}

func (r *TestRule) Process(interface{}) interface{} {
	return "a"
}

func (r *TestRule) WindowInterval() int {
	return 1
}

func (r *TestRule) String() string {
	return "TestRule"
}

func (r *TestRule) Window() ([]output.OutputEvent, error) {
	r.windowCounter++
	return []output.OutputEvent{}, nil
}

func TestWindowManager(t *testing.T) {
	testRule := &TestRule{}
	outChan := make(chan interface{})
	manager := &windowManager{
		outChan: &outChan,
	}
	config := &windowConfig{
		rule:     testRule,
		interval: 1,
	}

	manager.windowRunner(config)
	time.Sleep(1 * time.Second)
	manager.windowRunner(config)
	time.Sleep(1 * time.Second)
	manager.windowRunner(config)

	if testRule.windowCounter != 3 {
		t.Errorf("Expected Window() to be called 3 times, called %d times", testRule.windowCounter)
	}
}
