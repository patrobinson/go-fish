package main

import (
	"errors"
	"fmt"
	"plugin"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/patrobinson/go-fish/output"
)

// Rule is an interface for rule implementations
type Rule interface {
	Init()
	Process(interface{}) interface{}
	String() string
	WindowInterval() int
	Window() ([]output.OutputEvent, error)
	Close()
}

type ruleConfig struct {
	Source string `json:"source"`
	State  string `json:"state,omitempty"`
	Plugin string `json:"plugin"`
	Sink   string `json:"sink,omitempty"`
}

func NewRule(config ruleConfig) (Rule, error) {
	plug, err := plugin.Open(config.Plugin)
	if err != nil {
		return nil, fmt.Errorf("Unable to load plugin %s: %s", config.Plugin, err)
	}
	symRule, err := plug.Lookup("Rule")
	if err != nil {
		return nil, fmt.Errorf("Rule has no Rule symbol: %v", err)
	}
	rule, ok := symRule.(Rule)
	if !ok {
		return nil, errors.New("Rule is not a rule type")
	}
	rule.Init()
	return rule, nil
}

func startRule(rule Rule, output *chan interface{}, wg *sync.WaitGroup, windower *windowManager) *chan interface{} {
	input := make(chan interface{})
	log.Debugf("Starting %v\n", rule.String())

	if rule.WindowInterval() > 0 {
		windower.add(windowConfig{
			rule:     rule,
			interval: rule.WindowInterval(),
		})
	}
	(*wg).Add(1)
	go func(input *chan interface{}, output *chan interface{}, wg *sync.WaitGroup, r *Rule) {
		defer (*wg).Done()
		defer (*r).Close()
		for str := range *input {
			res := (*r).Process(str)
			*output <- res
		}
	}(&input, output, wg, &rule)

	windower.start()

	return &input
}
