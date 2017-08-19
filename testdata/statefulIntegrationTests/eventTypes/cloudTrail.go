package main

import (
	"encoding/json"

	"github.com/patrobinson/go-fish/event"
	es "github.com/patrobinson/go-fish/testdata/statefulIntegrationTests/eventStructs"
)

type cloudTrail struct{}

func (e cloudTrail) Decode(evt []byte) (event.Event, error) {
	var cloudTrail es.CloudTrail
	err := json.Unmarshal(evt, &cloudTrail)
	return cloudTrail, err
}

func (e cloudTrail) Name() string {
	return es.CloudTrailEventName
}

var EventType cloudTrail
