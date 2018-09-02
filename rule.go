package main

import (
	"errors"
	"fmt"
	"plugin"

	"github.com/patrobinson/go-fish/output"
	"github.com/patrobinson/go-fish/state"
	log "github.com/sirupsen/logrus"
)

// Rule is an interface for rule implementations
// Rules implement this interface as a plugin
type Rule interface {
	Init(...interface{}) error
	Process(interface{}) interface{}
	String() string
	WindowInterval() int
	Window() ([]output.OutputEvent, error)
	Close() error
}

type ruleConfig struct {
	Source string `json:"source"`
	State  string `json:"state,omitempty"`
	Plugin string `json:"plugin"`
	Sink   string `json:"sink,omitempty"`
}

func testRule(ruleFile string) error {
	plug, err := plugin.Open(ruleFile)
	if err != nil {
		return fmt.Errorf("Unable to load plugin: %s", err)
	}
	symRule, err := plug.Lookup("Rule")
	if err != nil {
		return fmt.Errorf("Rule has no Rule symbol: %v", err)
	}
	_ = symRule.(Rule)

	return nil
}

func newRule(config ruleConfig, s state.State) (Rule, error) {
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

func startRule(rule Rule, output *chan interface{}, windower *windowManager) *chan interface{} {
	input := make(chan interface{})
	log.Debugf("Starting %v\n", rule.String())

	go func(input *chan interface{}, output *chan interface{}, r Rule) {
		defer r.Close()
		for str := range *input {
			res := r.Process(str)
			*output <- res
		}
	}(&input, output, rule)

	if rule.WindowInterval() > 0 {
		windower.start()
	}

	return &input
}
