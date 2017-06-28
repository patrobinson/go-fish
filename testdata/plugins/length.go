package main

import "sync"
import "fmt"

type lengthRule string

func (r lengthRule) Process(thing interface{}) bool {
	foo, ok := thing.(string)
	if ok && len(foo) == 1 {
		return true
	}
	return false
}

func (r lengthRule) Start(input *chan interface{}, output *chan interface{}, wg *sync.WaitGroup) {
	defer (*wg).Done()
	for str := range *input {
		res := r.Process(str)
		*output <- res
	}
	fmt.Print("Length rule done\n")
}

func (r lengthRule) String() string { return string(r) }

var Rule lengthRule
