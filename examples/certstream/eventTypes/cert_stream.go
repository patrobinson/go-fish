package main

import (
	"encoding/json"

	"github.com/patrobinson/go-fish/event"
	es "github.com/patrobinson/go-fish/examples/certstream/eventStructs"
)

type certStream struct{}

func (e certStream) Decode(evt []byte) (event.Event, error) {
	var certStream es.CertStream
	err := json.Unmarshal(evt, &certStream)
	return certStream, err
}

func (e certStream) Name() string {
	return es.CertStreamEventName
}

var EventType certStream
