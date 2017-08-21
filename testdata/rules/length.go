package main

import (
	"github.com/patrobinson/go-fish/ruleHelpers"
	es "github.com/patrobinson/go-fish/testdata/eventStructs"
)

type lengthRule struct {
	rule_helpers.BasicRule
}

func (r *lengthRule) Process(thing interface{}) interface{} {
	foo, ok := thing.(es.ExampleType)
	if ok && len(foo.Str) == 1 {
		return true
	}
	return false
}

func (r *lengthRule) String() string { return "lengthRule" }

var Rule lengthRule
