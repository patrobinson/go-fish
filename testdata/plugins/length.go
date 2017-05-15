package main

type rule string

func (r rule) Process(thing interface{}) bool {
	foo, ok := thing.(string)
	if ok && len(foo) == 1 {
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

func (r rule) String() string { return "Length rule" }

var Rule rule
