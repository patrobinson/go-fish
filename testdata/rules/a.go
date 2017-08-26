package main

import (
	"github.com/patrobinson/go-fish/ruleHelpers"
	es "github.com/patrobinson/go-fish/testdata/eventStructs"
)

type aRule struct {
	rule_helpers.BasicRule
}

func (r *aRule) Process(thing interface{}) interface{} {
	foo, ok := thing.(es.ExampleType)
	if ok && foo.Str == "a" {
		return true
	}
	return false
}

func (r *aRule) String() string { return "aRule" }

var Rule aRule
