package state

import "sync"

// Counter is a simple in memory counter
type Counter struct {
	sync.RWMutex
	Count int
}

// Init initialises the counter
func (c *Counter) Init() error {
	c.Count = 0
	return nil
}

// Increment increments the counter
func (c *Counter) Increment() {
	c.Lock()
	c.Count++
	c.Unlock()
}

// Window returns the current value and resets the counter
func (c *Counter) Window() int {
	c.Lock()
	ret := c.Count
	c.Count = 0
	c.Unlock()
	return ret
}

// Close closes the counter (does nothing)
func (c *Counter) Close() {}
