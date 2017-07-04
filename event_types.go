package main

import (
	"errors"
	"path"
	"path/filepath"
	"plugin"

	log "github.com/Sirupsen/logrus"
	"github.com/patrobinson/go-fish/event"
)

type eventType interface {
	Name() string
	Decode([]byte) (event.Event, error)
}

func getEventTypes(eventFolder string) ([]eventType, error) {
	var events []eventType

	evtGlob := path.Join(eventFolder, "/*.so")
	evt, err := filepath.Glob(evtGlob)
	if err != nil {
		return events, err
	}

	var plugins []*plugin.Plugin
	for _, pFile := range evt {
		if plug, err := plugin.Open(pFile); err == nil {
			plugins = append(plugins, plug)
		}
	}

	for _, p := range plugins {
		symEvt, err := p.Lookup("EventType")
		if err != nil {
			log.Errorf("Event Type has no eventType symbol: %v", err)
			continue
		}
		e, ok := symEvt.(eventType)
		if !ok {
			log.Errorf("Event Type is not an Event interface type")
			continue
		}
		events = append(events, e)
	}

	log.Infof("Found %v event types", len(events))
	return events, nil
}

func matchEventType(eventTypes []eventType, rawEvt []byte) (event.Event, error) {
	var evt event.Event
	for _, et := range eventTypes {
		if evt, err := et.Decode(rawEvt); err == nil {
			return evt, nil
		}
	}
	return evt, errors.New("No Event Type matched")
}
