package output

import (
	"sync"
)

type ForwarderConfig struct {
	ForwardToChannel *chan interface{}
}

// Forwarder sinks are used to pass events from one channel to another
type ForwarderOutput struct {
	ForwardToChannel *chan interface{}
}

func (f *ForwarderOutput) Init() error {
	return nil
}

func (f *ForwarderOutput) Sink(input *chan interface{}, wg *sync.WaitGroup) {
	defer (*wg).Done()
	for message := range *input {
		*f.ForwardToChannel <- message
	}
}
