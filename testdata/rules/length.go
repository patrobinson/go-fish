package main

import (
	es "github.com/patrobinson/go-fish/testdata/eventStructs"
)

type lengthRule string

func (r *lengthRule) Init() {}

func (r *lengthRule) Process(thing interface{}) interface{} {
	foo, ok := thing.(es.ExampleType)
	if ok && len(foo.Str) == 1 {
		return true
	}
	return false
}

func (r *lengthRule) String() string { return string(*r) }

func (r *lengthRule) Close() {}

var Rule lengthRule
