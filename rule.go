package main

import (
	"errors"
	"fmt"
	"plugin"
	"sync"

	"github.com/patrobinson/go-fish/output"
	"github.com/patrobinson/go-fish/state"
	log "github.com/sirupsen/logrus"
)

// Rule is an interface for rule implementations
type Rule interface {
	Init(state.State) error
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

func NewRule(config ruleConfig, s state.State) (Rule, error) {
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
	if err := rule.Init(s); err != nil {
		return nil, err
	}
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
	go func(input *chan interface{}, output *chan interface{}, wg *sync.WaitGroup, r Rule) {
		defer (*wg).Done()
		defer r.Close()
		for str := range *input {
			res := r.Process(str)
			*output <- res
		}
	}(&input, output, wg, rule)

	windower.start()

	return &input
}
