package state

import "sync"

type Counter struct {
	sync.RWMutex
	Counter int
}

func (c *Counter) Increment() {
	c.Lock()
	c.Counter++
	c.Unlock()
}

func NewCounter() *Counter {
	return &Counter{
		Counter: 0,
	}
}
