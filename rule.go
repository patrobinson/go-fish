package main

import (
	"path"
	"path/filepath"
	"plugin"
	"sync"

	log "github.com/Sirupsen/logrus"
)

// Rule is an interface for rule implementations
type Rule interface {
	Init()
	Process(interface{}) interface{}
	String() string
	Close()
}

func startRules(rulesFolder string, output *chan interface{}, wg *sync.WaitGroup) []*chan interface{} {
	pluginGlob := path.Join(rulesFolder, "/*.so")
	plugins, err := filepath.Glob(pluginGlob)
	if err != nil {
		log.Fatal(err)
	}

	var rules []*plugin.Plugin
	for _, pFile := range plugins {
		if plug, err := plugin.Open(pFile); err == nil {
			rules = append(rules, plug)
		} else {
			log.Errorf("Unable to load Plugin %v: %v", pFile, err)
		}
	}

	log.Infof("Found %v rules", len(rules))

	var inputs []*chan interface{}
	for _, r := range rules {
		symRule, err := r.Lookup("Rule")
		if err != nil {
			log.Errorf("Rule has no Rule symbol: %v", err)
			continue
		}
		rule, ok := symRule.(Rule)
		if !ok {
			log.Errorf("Rule is not a rule type.")
			continue
		}
		rule.Init()

		input := make(chan interface{})
		inputs = append(inputs, &input)
		log.Debugf("Starting %v\n", rule.String())
		(*wg).Add(1)
		go func(input *chan interface{}, output *chan interface{}, wg *sync.WaitGroup, r *Rule) {
			defer (*wg).Done()
			defer (*r).Close()
			for str := range *input {
				res := (*r).Process(str)
				*output <- res
			}
		}(&input, output, wg, &rule)
	}

	return inputs
}
