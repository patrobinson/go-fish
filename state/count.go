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

func (c *Counter) Window() int {
	c.Lock()
	ret := c.Counter
	c.Counter = 0
	c.Unlock()
	return ret
}

func NewCounter() *Counter {
	return &Counter{
		Counter: 0,
	}
}
