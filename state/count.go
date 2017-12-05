package state

import "sync"

type Counter struct {
	sync.RWMutex
	Count int
}

func (c *Counter) Init() error {
	c.Count = 0
	return nil
}

func (c *Counter) Increment() {
	c.Lock()
	c.Count++
	c.Unlock()
}

func (c *Counter) Window() int {
	c.Lock()
	ret := c.Count
	c.Count = 0
	c.Unlock()
	return ret
}

func NewCounter() *Counter {
	return &Counter{
		Count: 0,
	}
}

func (c *Counter) Close() error {
	return nil
}
