package main

type aRule string

func init() {
	Rule = "a"
}

func (r aRule) Process(thing interface{}) bool {
	foo, ok := thing.(string)
	if ok && foo == "a" {
		return true
	}
	return false
}

func (r aRule) Start(input chan interface{}, output chan interface{}) {
	for str := range input {
		res := r.Process(str)
		output <- res
	}
}

func (r aRule) String() string { return string(r) }

var Rule aRule
