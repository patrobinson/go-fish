package main

type lengthRule string

func init() {
	Rule = "Length"
}

func (r lengthRule) Process(thing interface{}) bool {
	foo, ok := thing.(string)
	if ok && len(foo) == 1 {
		return true
	}
	return false
}

func (r lengthRule) Start(input chan interface{}, output chan interface{}) {
	for str := range input {
		res := r.Process(str)
		output <- res
	}
}

func (r lengthRule) String() string { return string(r) }

var Rule lengthRule
