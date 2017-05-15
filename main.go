package main

import (
	"plugin"
	"os"
	"path/filepath"
	log "github.com/Sirupsen/logrus"
	"path"
	"github.com/patrobinson/go-fish/input"
	"github.com/patrobinson/go-fish/output"
)

type Rule interface {
	Start(chan interface{}, chan interface{})
	Process(interface{}) bool
	String() string
}

type Input interface {
	Retrieve(chan interface{})
}

type Output interface {
	Sink(chan interface{})
}

func main() {
	plugin_folder := os.Args[1]
	inFile := os.Args[2]
	outFile := os.Args[3]
	in := input.FileInput{FileName: inFile}
	out := output.FileOutput{FileName: outFile}

	run(plugin_folder, in, out)
}

func run(plugin_folder string, in interface{}, out interface{}) {
	log.SetLevel(log.DebugLevel)
	outChan := startOutput(out)
	rChans := startRules(plugin_folder, outChan)
	inChan := startInput(in)

	// receive from inputs and send to all rules
	go func(in chan interface{}, ruleChans []chan interface{}) {
		for data := range in {
			for _, i := range ruleChans {
				i <- data
			}
		}
	}(inChan, rChans)
}

func startOutput(out interface{}) chan interface{} {
	outChan := make(chan interface{})
	outSender := out.(Output)
	go outSender.Sink(outChan)
	return outChan
}

func startInput(in interface{}) chan interface{} {
	inChan := make(chan interface{})
	inReceiver := in.(Input)
	go inReceiver.Retrieve(inChan)
	return inChan
}

func startRules(plugin_folder string, output chan interface{}) []chan interface{} {
	plugin_glob := path.Join(plugin_folder, "/*.so")
	plugins, err := filepath.Glob(plugin_glob)
	if err != nil {
		log.Fatal(err)
	}

	var rules []*plugin.Plugin
	for _, p_file := range plugins {
		if plug, err := plugin.Open(p_file); err == nil {
			rules = append(rules, plug)
		}
	}

	var inputs []chan interface{}
	for _, r := range rules {
		symRule, err := r.Lookup("Rule")
		if err != nil {
			log.Errorf("Rule has no Rule symbol: %v", err)
			continue
		}
		var rule Rule
		rule, ok := symRule.(Rule)
		if !ok {
			log.Errorf("Rule is not a rule type. Does it implement the Process() function?")
			continue
		}
		input := make(chan interface{})
		inputs = append(inputs, input)
		log.Debugf("Starting %v\n", rule)
		go rule.Start(input, output)
	}

	return inputs
}
