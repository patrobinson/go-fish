package main

import (
	"testing"
	"sync"
	"fmt"
)

type testInput struct {
	value string
}

func (t testInput) Retrieve(out *chan []byte) {
	defer close(*out)
	*out <- []byte(t.value)
}

type testOutput struct {
	c *chan bool
}

func (t testOutput) Sink(in *chan interface{}, wg *sync.WaitGroup) {
	defer (*wg).Done()
	for msg := range *in {
		fmt.Println("Input received")
		*t.c <- msg.(bool)
	}
	fmt.Println("Input closed")
}

func TestSuccessfulRun(t *testing.T) {
	output := make(chan bool)
	o := testOutput{c: &output}
	in := testInput{value: "a"}
	go run("testdata/rules", in, o)
	r1 := <-output
	fmt.Print("Received 1 output")
	r2 := <-output
	fmt.Print("Received 2 output")
	if !r1 && !r2 {
		t.Errorf("Rules did not match %v %v", r1, r2)
	}
}

func TestFailRun(t *testing.T) {
	output := make(chan bool)
	o := testOutput{c: &output}
	in := testInput {value: "abc"}
	go run("testdata/rules", in, o)
	if r1, r2 := <-output, <-output; r1 || r2 {
		t.Errorf("Rules did not match %v %v", r1, r2)
	}
}
