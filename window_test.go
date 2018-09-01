package main

import (
	"testing"
	"time"

	"github.com/patrobinson/go-fish/output"
)

type TestRule struct {
	windowCounter int
}

func (r *TestRule) Init(...interface{}) error { return nil }

func (r *TestRule) Close() error { return nil }

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
		sinkChan: &outChan,
		rule:     testRule,
	}

	manager.windowRunner()
	time.Sleep(1 * time.Second)
	manager.windowRunner()
	time.Sleep(1 * time.Second)
	manager.windowRunner()

	if testRule.windowCounter != 3 {
		t.Errorf("Expected Window() to be called 3 times, called %d times", testRule.windowCounter)
	}
}
