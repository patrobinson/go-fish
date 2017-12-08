package main

import (
	"encoding/json"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"

	log "github.com/Sirupsen/logrus"
	"github.com/patrobinson/go-fish/input"
	"github.com/patrobinson/go-fish/output"
)

// PipelineConfig forms the basic configuration of our processor
type PipelineConfig struct {
	EventFolder string                        `json:"eventFolder"`
	Rules       map[string]ruleConfig         `json:"rules"`
	States      map[string]stateConfig        `json:"states"`
	Sources     map[string]input.SourceConfig `json:"sources"`
	Sinks       map[string]output.SinkConfig  `json:"sinks"`
}

type stateConfig struct {
	Type string `json:"type"`
}

func parseConfig(configFile io.Reader) (PipelineConfig, error) {
	var config PipelineConfig
	jsonParser := json.NewDecoder(configFile)
	err := jsonParser.Decode(&config)
	return config, err
}

// Pipeline is a Directed Acyclic Graph
type Pipeline struct {
	Rules         map[string]*RuleMapper
	States        map[string]State
	Sources       map[string]*SourceMapper
	Sinks         map[string]*SinkMapper
	outWaitGroup  *sync.WaitGroup
	ruleWaitGroup *sync.WaitGroup
	outChannel    *chan interface{}
	inChannel     *chan []byte
	windower      *windowManager
	eventFolder   string
}

type RuleMapper struct {
	Rule             Rule
	Sink             *SinkMapper
	RuleInputChannel *chan interface{}
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
	var pipeline *Pipeline
	pipeline.eventFolder = config.EventFolder

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

	for name, ruleConfig := range config.Rules {
		rule, err := NewRule(ruleConfig)
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
	p.windower = &windowManager{
		outChan: p.outChannel,
	}
	p.outWaitGroup = &sync.WaitGroup{}
	p.ruleWaitGroup = &sync.WaitGroup{}

	for _, sink := range p.Sinks {
		err := output.StartOutput(&sink.Sink, p.outWaitGroup, sink.OutChannel)
		if err != nil {
			return err
		}
	}

	for _, rule := range p.Rules {
		rule.RuleInputChannel = startRule(rule.Rule, rule.Sink.OutChannel, p.ruleWaitGroup, p.windower)
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
		go func(iChan *chan []byte, rules []*RuleMapper) {
			for data := range *iChan {
				evt, err := matchEventType(eventTypes, data)
				if err != nil {
					log.Infof("Error matching event: %v", err)
				}
				for _, rule := range rules {
					*rule.RuleInputChannel <- evt
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
}
