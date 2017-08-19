package main

import (
	es "github.com/patrobinson/go-fish/testdata/eventStructs"
)

type aRule string

func (r *aRule) Init() {}

func (r *aRule) Process(thing interface{}) interface{} {
	foo, ok := thing.(es.ExampleType)
	if ok && foo.Str == "a" {
		return true
	}
	return false
}

func (r *aRule) String() string { return string(*r) }

func (r *aRule) Close() {}

var Rule aRule
