package main

import (
	"github.com/patrobinson/go-fish/event"
	es "github.com/patrobinson/go-fish/testdata/eventStructs"
)

type exampleEventType struct {}

func (e exampleEventType) Decode(evt []byte) (event.Event, error) {
	decodedEvent := es.ExampleType{
		Str: string(evt),
	}
	return decodedEvent, nil
}

func (e exampleEventType) Name() string {
	return es.EventName
}

var EventType exampleEventType
