package main

import (
	"testing"
)

type testInput struct {
	value string
}

func (t testInput) Retrieve(out chan interface{}) {
	defer close(out)
	out <- t.value
}

type testOutput struct {
	c chan bool
}

func (t testOutput) Sink(in chan interface{}) {
	for i := range in {
		result := i.(bool)
		t.c <- result
	}
}

func TestSuccessfulRun(t *testing.T) {
	output := make(chan bool)
	o := testOutput{c: output}
	in := testInput{value: "a"}
	run("testdata/plugins", in, o)
	if r1, r2 := <-output, <-output; !r1 && !r2 {
		t.Errorf("Rules did not match %v %v", r1, r2)
	}
}

func TestFailRun(t *testing.T) {
	output := make(chan bool)
	o := testOutput{c: output}
	in := testInput {value: "abc"}
	run("testdata/plugins", in, o)
	if r1, r2 := <-output, <-output; r1 || r2 {
		t.Errorf("Rules did not match %v %v", r1, r2)
	}
}
