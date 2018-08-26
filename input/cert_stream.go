package input

import (
	"encoding/json"

	certstream "github.com/CaliDog/certstream-go"
	"github.com/jmoiron/jsonq"
	log "github.com/sirupsen/logrus"
)

// CertStreamInput provides a stream of Certificate Transparency Logs https://www.certificate-transparency.org/
// It's used for example purposes only and should NOT be used in production
// Since the stream is a websocket slow processing would result in memory pressure
// Instead the data should be pushed into a queue, such as Kinesis, and GoFish should read from that stream
type CertStreamInput struct {
	stream chan jsonq.JsonQuery
}

func (c *CertStreamInput) Init() error {
	c.stream = certstream.CertStreamEventStream(false)
	return nil
}

func (c *CertStreamInput) Retrieve(output *chan interface{}) {
	defer close(*output)
	for i := range c.stream {
		j, err := i.Object()
		if err != nil {
			log.Errorf("Invalid data from Cert Stream: %s", err)
			continue
		}
		data, err := json.Marshal(j)
		if err != nil {
			log.Errorf("Unable to Marshal Cert Stream: %s", err)
			continue
		}
		*output <- data
	}
}

func (c *CertStreamInput) Close() error {
	close(c.stream)
	return nil
}
