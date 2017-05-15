package main

type rule string

func (r rule) Process(thing interface{}) bool {
	foo, ok := thing.(string)
	if ok && foo == "a" {
		return true
	}
	return false
}

func (r rule) Start(input chan interface{}, output chan interface{}) {
	for str := range input {
		res := r.Process(str)
		output <- res
	}
}

func (r rule) String() string { return "A rule" }

var Rule rule
