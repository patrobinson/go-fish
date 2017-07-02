package main

import (
	"sync"
	"fmt"
	es "github.com/patrobinson/go-fish/testdata/eventStructs"
)

type aRule string

func (r aRule) Process(thing interface{}) bool {
	foo, ok := thing.(es.ExampleType)
	if ok && foo.Str == "a" {
		return true
	}
	return false
}

func (r aRule) Start(input *chan interface{}, output *chan interface{}, wg *sync.WaitGroup) {
	defer (*wg).Done()
	for str := range *input {
		res := r.Process(str)
		*output <- res
	}
	fmt.Print("A rule done\n")
}

func (r aRule) String() string { return string(r) }

var Rule aRule
