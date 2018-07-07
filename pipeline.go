package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"sync"
	"syscall"
	"time"

	"github.com/boltdb/bolt"
	"github.com/google/uuid"
	"github.com/patrobinson/go-fish/input"
	"github.com/patrobinson/go-fish/output"
	"github.com/patrobinson/go-fish/state"
	log "github.com/sirupsen/logrus"
)

// PipelineConfig forms the basic configuration of our processor
type PipelineConfig struct {
	EventFolder string                        `json:"eventFolder"`
	Rules       map[string]ruleConfig         `json:"rules"`
	States      map[string]state.StateConfig  `json:"states"`
	Sources     map[string]input.SourceConfig `json:"sources"`
	Sinks       map[string]output.SinkConfig  `json:"sinks"`
}

func parseConfig(rawConfig []byte) (PipelineConfig, error) {
	var config PipelineConfig
	err := json.Unmarshal(rawConfig, &config)
	log.Debugf("Config Parsed: ", config)
	return config, err
}

func validateConfig(config PipelineConfig) error {
	// Validate that any Sources, Sinks and States a Rule points to exist
	for ruleName, rule := range config.Rules {
		// TODO: Ensure a source exists
		if _, ok := config.Sources[rule.Source]; !ok {
			if _, ok := config.Rules[rule.Source]; !ok {
				return fmt.Errorf("Invalid source for rule %s: %s", ruleName, rule.Source)
			}
		}

		_, ok := config.Sinks[rule.Sink]
		if rule.Sink != "" && !ok {
			if _, ok := config.Rules[rule.Sink]; !ok {
				return fmt.Errorf("Invalid sink for rule %s: %s", ruleName, rule.Sink)
			}
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
	ID            uuid.UUID
	Config        []byte
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
	Rules            []*RuleMapper
}

type SourceMapper struct {
	SourceChannel *chan []byte
	Source        input.Source
	Rules         []*RuleMapper
}

type SinkMapper struct {
	SinkChannel *chan interface{}
	Sink        output.Sink
}

func makeSource(sourceConfig input.SourceConfig) (*SourceMapper, error) {
	sourceChan := make(chan []byte)
	source, err := input.Create(sourceConfig)
	if err != nil {
		return nil, err
	}
	return &SourceMapper{
		SourceChannel: &sourceChan,
		Source:        source,
	}, nil
}

func makeSink(sinkConfig output.SinkConfig) (*SinkMapper, error) {
	sinkChan := make(chan interface{})
	sink, err := output.Create(sinkConfig)
	if err != nil {
		return nil, err
	}
	return &SinkMapper{
		SinkChannel: &sinkChan,
		Sink:        sink,
	}, nil
}

type PipelineManager struct {
	backendConfig
	Backend backend
}

func (p *PipelineManager) Init() error {
	var err error
	p.Backend, err = p.backendConfig.Create()
	if err != nil {
		return err
	}
	return p.Backend.Init()
}

func (p *PipelineManager) Store(pipeline *Pipeline) error {
	return p.Backend.Store(pipeline)
}

func (p *PipelineManager) Get(uuid []byte) ([]byte, error) {
	return p.Backend.Get(uuid)
}

func (p *PipelineManager) NewPipeline(rawConfig []byte) (*Pipeline, error) {
	config, err := parseConfig(rawConfig)
	if err != nil {
		return nil, err
	}
	validateConfig(config)

	pipeline := &Pipeline{
		ID:          uuid.New(),
		Config:      rawConfig,
		eventFolder: config.EventFolder,
		Sources:     make(map[string]*SourceMapper),
		Rules:       make(map[string]*RuleMapper),
		States:      make(map[string]state.State),
		Sinks:       make(map[string]*SinkMapper),
	}

	for sourceName, sourceConfig := range config.Sources {
		var err error
		pipeline.Sources[sourceName], err = makeSource(sourceConfig)
		if err != nil {
			return nil, err
		}
	}

	for sinkName, sinkConfig := range config.Sinks {
		var err error
		pipeline.Sinks[sinkName], err = makeSink(sinkConfig)
		if err != nil {
			return nil, err
		}
	}

	for stateName, stateConfig := range config.States {
		var err error
		pipeline.States[stateName], err = state.Create(stateConfig)
		if err != nil {
			return nil, err
		}
	}

	for ruleName, ruleConfig := range config.Rules {
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

		// If the sink is a rule
		if _, ok := config.Rules[ruleConfig.Sink]; ok {
			intermediateChan := make(chan interface{})
			destinationRuleName := ruleConfig.Sink
			var sourceRules []*RuleMapper
			if sourceM, ok := pipeline.Sources[ruleName]; ok {
				sourceRules = sourceM.Rules
			}
			pipeline.Sources[ruleName], err = makeSource(input.SourceConfig{
				Type: "Forward",
				ForwarderConfig: input.ForwarderConfig{
					ForwardToChannel: &intermediateChan,
				},
			})
			if err != nil {
				return nil, err
			}

			// If the destination rule was created first, restore the rule mapper to the source we just created
			pipeline.Sources[ruleName].Rules = sourceRules

			pipeline.Sinks[destinationRuleName], err = makeSink(output.SinkConfig{
				Type: "Forward",
				ForwarderConfig: output.ForwarderConfig{
					ForwardToChannel: &intermediateChan,
				},
			})
			if err != nil {
				return nil, err
			}

			destinationRuleMapping := pipeline.Rules[destinationRuleName]
			pipeline.Sources[ruleName].Rules = append(pipeline.Sources[ruleName].Rules, destinationRuleMapping)
		}

		ruleMapping := &RuleMapper{
			Rule: rule,
			Sink: pipeline.Sinks[ruleConfig.Sink],
		}

		pipeline.Rules[ruleName] = ruleMapping
		if _, ok := pipeline.Sources[ruleConfig.Source]; !ok {
			// In the case the Source is another Rule, the source may not exist yet
			pipeline.Sources[ruleConfig.Source] = &SourceMapper{}
		}
		pipeline.Sources[ruleConfig.Source].Rules = append(pipeline.Sources[ruleConfig.Source].Rules, ruleMapping)
	}

	for sourceName, sourceConfig := range pipeline.Sources {
		if sourceConfig.Source == nil {
			return &Pipeline{}, fmt.Errorf("Source %s referred to but does not exist", sourceName)
		}
	}

	err = p.Store(pipeline)
	if err != nil {
		return nil, err
	}

	return pipeline, nil
}

func (p *Pipeline) StartPipeline() error {
	p.outWaitGroup = &sync.WaitGroup{}
	p.ruleWaitGroup = &sync.WaitGroup{}

	for _, sink := range p.Sinks {
		err := output.StartOutput(&sink.Sink, p.outWaitGroup, sink.SinkChannel)
		if err != nil {
			return err
		}
	}

	for ruleName, rule := range p.Rules {
		log.Infof("Starting rule %s", ruleName)
		(*rule).WindowManager = &windowManager{
			sinkChan: rule.Sink.SinkChannel,
		}
		(*rule).RuleInputChannel = startRule(rule.Rule, rule.Sink.SinkChannel, p.ruleWaitGroup, rule.WindowManager)
	}

	for sourceName, source := range p.Sources {
		log.Infoln("Starting source", sourceName)
		err := input.StartInput((*source).Source, (*source).SourceChannel)
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
		}(source.SourceChannel, source.Rules)
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
		close(*o.SinkChannel)
	}
	p.outWaitGroup.Wait()

	log.Debug("Closing state files\n")
	for _, s := range p.States {
		s.Close()
	}
}

func startBoltDB(databaseName string, bucketName string) (*bolt.DB, error) {
	db, err := bolt.Open(databaseName, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return db, err
	}

	return db, db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return err
		}
		return nil
	})
}
