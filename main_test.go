package main

import (
	"fmt"
	"sync"
	"testing"

	log "github.com/Sirupsen/logrus"
)

type testInput struct {
	value string
}

func (t testInput) Init() error {
	return nil
}

func (t *testInput) Retrieve(out *chan []byte) {
	defer close(*out)
	*out <- []byte(t.value)
}

type testOutput struct {
	c *chan bool
}

func (t *testOutput) Sink(in *chan interface{}, wg *sync.WaitGroup) {
	defer (*wg).Done()
	for msg := range *in {
		log.Info("Input received")
		*t.c <- msg.(bool)
	}
	log.Info("Input closed")
}

func TestSuccessfulRun(t *testing.T) {
	output := make(chan bool)
	out := &testOutput{c: &output}
	in := &testInput{value: "a"}
	go run("testdata/rules", "testdata/eventTypes", in, out)
	r1 := <-output
	fmt.Print("Received 1 output\n")
	r2 := <-output
	fmt.Print("Received 2 output\n")
	if !r1 || !r2 {
		t.Errorf("Rules did not match %v %v", r1, r2)
	}
}

func TestFailRun(t *testing.T) {
	output := make(chan bool)
	out := &testOutput{c: &output}
	in := &testInput{value: "abc"}
	go run("testdata/rules", "testdata/eventTypes", in, out)
	if r1, r2 := <-output, <-output; r1 || r2 {
		t.Errorf("Rules did not match %v %v", r1, r2)
	}
}

type benchmarkInput struct {
	input *chan []byte
}

func (t benchmarkInput) Init() error {
	return nil
}

func (t *benchmarkInput) Retrieve(out *chan []byte) {
	defer close(*out)
	for in := range *t.input {
		*out <- in
	}
}

func BenchmarkRun(b *testing.B) {
	log.SetLevel(log.WarnLevel)
	output := make(chan bool)
	out := &testOutput{c: &output}

	input := make(chan []byte)
	in := &benchmarkInput{input: &input}
	var r1 bool
	var r2 bool
	b.ResetTimer()
	go run("testdata/rules", "testdata/eventTypes", in, out)
	for i := 0; i < b.N; i++ {
		bs := make([]byte, 1)
		bs[0] = byte(i)
		input <- bs
		r1 = <-output
		r2 = <-output
	}
	r := r1 || r2
	fmt.Printf("%v\n", r)
}
