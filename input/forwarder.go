package input

import(
	log "github.com/Sirupsen/logrus"
)

type ForwarderConfig struct {
	ForwardToChannel *chan interface{}
}

type ForwarderInput struct {
	ForwardToChannel *chan interface{}
}

func (f *ForwarderInput) Init() error {
	return nil
}

func (f *ForwarderInput) Retrieve(output *chan []byte) {
	for msg := range *f.ForwardToChannel {
		msgByte, ok := msg.([]byte)
		if !ok {
			log.Fatal("Unable to assert message from rule is of type byte")
		}
		*output <- msgByte
	}
}
