package main

import (
	"plugin"
	"path"
	"path/filepath"
	log "github.com/Sirupsen/logrus"
	"github.com/patrobinson/go-fish/event"
	"errors"
)


type EventType interface {
	Name() string
	Decode([]byte) (event.Event, error)
}

func getEventTypes(event_folder string) ([]EventType, error) {
	var events []EventType

	evt_glob := path.Join(event_folder, "/*.so")
	evt, err := filepath.Glob(evt_glob)
	if err != nil {
		return events, err
	}

	var plugins []*plugin.Plugin
	for _, p_file := range evt {
		if plug, err := plugin.Open(p_file); err == nil {
			plugins = append(plugins, plug)
		}
	}

	for _, p := range plugins {
		symEvt, err := p.Lookup("EventType")
		if err != nil {
			log.Errorf("Event Type has no eventType symbol: %v", err)
			continue
		}
		e, ok := symEvt.(EventType)
		if !ok {
			log.Errorf("Event Type is not an Event interface type")
			continue
		}
		events = append(events, e)
	}

	log.Infof("Found %v event types", len(events))
	return events, nil
}

func matchEventType(eventTypes []EventType, rawEvt []byte) (event.Event, error) {
	var evt event.Event
	for _, et := range eventTypes {
		if evt, err := et.Decode(rawEvt); err == nil {
			return evt, nil
		}
	}
	return evt, errors.New("No Event Type matched")
}
