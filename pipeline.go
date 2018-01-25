package main

import (
	"fmt"
	"encoding/json"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"reflect"

	log "github.com/Sirupsen/logrus"
	"github.com/patrobinson/go-fish/input"
	"github.com/patrobinson/go-fish/output"
	"github.com/patrobinson/go-fish/state"
)

// PipelineConfig forms the basic configuration of our processor
type PipelineConfig struct {
	EventFolder string                        `json:"eventFolder"`
	Rules       map[string]ruleConfig         `json:"rules"`
	States      map[string]state.StateConfig  `json:"states"`
	Sources     map[string]input.SourceConfig `json:"sources"`
	Sinks       map[string]output.SinkConfig  `json:"sinks"`
}

func parseConfig(configFile io.Reader) (PipelineConfig, error) {
	var config PipelineConfig
	jsonParser := json.NewDecoder(configFile)
	err := jsonParser.Decode(&config)
	log.Debugf("Config Parsed: ", config)
	return config, err
}

func validateConfig(config PipelineConfig) error {
	// Validate that any Sources, Sinks and States a Rule points to exist
	for ruleName, rule := range config.Rules {
		if _, ok := config.Sources[rule.Source]; !ok {
			return fmt.Errorf("Invalid source for rule %s: %s", ruleName, rule.Source)
		}

		_, ok := config.Sinks[rule.Sink]
		if rule.Sink != "" && !ok {
			return fmt.Errorf("Invalid sink for rule %s: %s", ruleName, rule.Sink)
		}

		_, ok = config.States[rule.State]
		if rule.State != "" && !ok {
			return fmt.Errorf("Invalid state for rule %s: %s", ruleName, rule.State)
		}

		if _, err := os.Stat(rule.Plugin); err != nil {
			return fmt.Errorf("Invalid plugin: %s", err)
		}
	}

	// Validate there are no naming conflicts
	var keys []reflect.Value
	keys = append(keys, reflect.ValueOf(config.Sources).MapKeys()...)
	keys = append(keys, reflect.ValueOf(config.Rules).MapKeys()...)
	keys = append(keys, reflect.ValueOf(config.Sinks).MapKeys()...)
	keys = append(keys, reflect.ValueOf(config.States).MapKeys()...)
	duplicates := findDuplicates(keys)
	if len(duplicates) > 0 {
		return fmt.Errorf("Invalid configuration, duplicate keys: %s", duplicates)
	}

	return nil
}

func findDuplicates(s []reflect.Value) []string {
	var result []string
	strings := make(map[string]bool)
	for _, str := range s {
		if strings[str.String()] == true {
			result = append(result, str.String())
		} else {
			strings[str.String()] = true
		}
	}
	return result
}

// Pipeline is a Directed Acyclic Graph
type Pipeline struct {
	Rules         map[string]*RuleMapper
	States        map[string]state.State
	Sources       map[string]*SourceMapper
	Sinks         map[string]*SinkMapper
	outWaitGroup  *sync.WaitGroup
	ruleWaitGroup *sync.WaitGroup
	eventFolder   string
}

type RuleMapper struct {
	Rule             Rule
	Sink             *SinkMapper
	RuleInputChannel *chan interface{}
	WindowManager    *windowManager
	State            *state.State
}

type SourceMapper struct {
	InChannel *chan []byte
	Source    input.Source
	Rules     []*RuleMapper
}

type SinkMapper struct {
	OutChannel *chan interface{}
	Sink       output.Sink
}

func NewPipeline(config PipelineConfig) (*Pipeline, error) {
	pipeline := &Pipeline{
		eventFolder: config.EventFolder,
		Sources:     make(map[string]*SourceMapper),
		Rules:       make(map[string]*RuleMapper),
		States:      make(map[string]state.State),
		Sinks:       make(map[string]*SinkMapper),
	}

	for name, sourceConfig := range config.Sources {
		inChan := make(chan []byte)
		source, err := input.Create(sourceConfig)
		if err != nil {
			return nil, err
		}
		pipeline.Sources[name] = &SourceMapper{
			InChannel: &inChan,
			Source:    source,
		}
	}

	for name, sinkConfig := range config.Sinks {
		outChan := make(chan interface{})
		sink, err := output.Create(sinkConfig)
		if err != nil {
			return nil, err
		}
		pipeline.Sinks[name] = &SinkMapper{
			OutChannel: &outChan,
			Sink:       sink,
		}
	}

	for name, stateConfig := range config.States {
		var err error
		pipeline.States[name], err = state.Create(stateConfig)
		if err != nil {
			return nil, err
		}
	}

	for name, ruleConfig := range config.Rules {
		var ruleState state.State
		if ruleConfig.State != "" {
			ruleState = pipeline.States[ruleConfig.State]
		} else {
			ruleState = nil
		}

		rule, err := NewRule(ruleConfig, ruleState)
		if err != nil {
			return nil, err
		}
		ruleMapping := &RuleMapper{
			Rule: rule,
			Sink: pipeline.Sinks[ruleConfig.Sink],
		}

		pipeline.Rules[name] = ruleMapping
		pipeline.Sources[ruleConfig.Source].Rules = append(pipeline.Sources[ruleConfig.Source].Rules, ruleMapping)
	}

	return pipeline, nil
}

func (p *Pipeline) StartPipeline() error {
	p.outWaitGroup = &sync.WaitGroup{}
	p.ruleWaitGroup = &sync.WaitGroup{}

	for _, sink := range p.Sinks {
		err := output.StartOutput(&sink.Sink, p.outWaitGroup, sink.OutChannel)
		if err != nil {
			return err
		}
	}

	for ruleName, rule := range p.Rules {
		log.Infof("Starting rule %s", ruleName)
		(*rule).WindowManager = &windowManager{
			outChan: rule.Sink.OutChannel,
		}
		(*rule).RuleInputChannel = startRule(rule.Rule, rule.Sink.OutChannel, p.ruleWaitGroup, rule.WindowManager)
	}

	for _, source := range p.Sources {
		err := input.StartInput(&source.Source, source.InChannel)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *Pipeline) Run() {
	eventTypes, err := getEventTypes(p.eventFolder)
	if err != nil {
		log.Fatalf("Failed to get Event plugins: %v", err)
	}

	for _, source := range p.Sources {
		go func(iChan *chan []byte, ruleMaps []*RuleMapper) {
			for data := range *iChan {
				evt, err := matchEventType(eventTypes, data)
				if err != nil {
					log.Infof("Error matching event: %v", err)
				}
				for _, ruleMap := range ruleMaps {
					*ruleMap.RuleInputChannel <- evt
				}
			}
		}(source.InChannel, source.Rules)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	for sig := range c {
		log.Infof("Received %s signal... exiting\n", sig)
		break
	}

	p.Close()
}

func (p *Pipeline) Close() {
	log.Debug("Closing rule channels\n")

	for _, r := range p.Rules {
		close(*r.RuleInputChannel)
	}
	p.ruleWaitGroup.Wait()

	log.Debug("Closing output channels\n")
	for _, o := range p.Sinks {
		close(*o.OutChannel)
	}
	p.outWaitGroup.Wait()

	log.Debug("Closing state files\n")
	for _, s := range p.States {
		s.Close()
	}
}
