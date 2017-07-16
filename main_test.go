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

func BenchmarkRun(b *testing.B) {
	log.SetLevel(log.WarnLevel)
	output := make(chan bool)
	out := &testOutput{c: &output}
	var r1 bool
	var r2 bool
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		in := &testInput{value: string(n)}
		go run("testdata/rules", "testdata/eventTypes", in, out)
		r1 = <-output
		r2 = <-output
	}
	result := r1 || r2
	fmt.Printf("%v\n", result)

}
