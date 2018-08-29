package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"time"

	"github.com/boltdb/bolt"
	"github.com/google/uuid"
	"github.com/patrobinson/go-fish/input"
	"github.com/patrobinson/go-fish/output"
	"github.com/patrobinson/go-fish/state"
	log "github.com/sirupsen/logrus"
)

// pipelineConfig forms the basic configuration of our processor
type pipelineConfig struct {
	EventFolder string                        `json:"eventFolder"`
	Rules       map[string]ruleConfig         `json:"rules"`
	States      map[string]state.StateConfig  `json:"states"`
	Sources     map[string]input.SourceConfig `json:"sources"`
	Sinks       map[string]output.SinkConfig  `json:"sinks"`
}

func parseConfig(rawConfig []byte) (pipelineConfig, error) {
	var config pipelineConfig
	err := json.Unmarshal(rawConfig, &config)
	log.Debugf("Config Parsed: ", config)
	return config, err
}

func validateConfig(config pipelineConfig) error {
	stateUsage := make(map[string]int)
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
		if rule.State != "" {
			if !ok {
				return fmt.Errorf("Invalid state for rule %s: %s", ruleName, rule.State)
			}
			stateUsage[rule.State]++
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

	// Validate no rules share a state
	for state, used := range stateUsage {
		if used > 1 {
			return fmt.Errorf("Invalid rule configuration, only one rule can use each state but found multiple use state: %s", state)
		}
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

// pipeline is a Directed Acyclic Graph
type pipeline struct {
	ID          uuid.UUID
	Config      []byte
	Nodes       map[string]*pipelineNode
	eventFolder string
}

func (p *pipeline) addVertex(name string, vertex *pipelineNode) {
	p.Nodes[name] = vertex
}

func (p *pipeline) addEdge(from, to *pipelineNode) {
	from.AddChild(to)
	to.AddParent(from)
}

func (p *pipeline) sources() []*pipelineNode {
	var sources []*pipelineNode
	for _, node := range p.Nodes {
		if node.InDegree() == 0 {
			sources = append(sources, node)
		}
	}
	return sources
}

func (p *pipeline) internals() map[string]*pipelineNode {
	internals := make(map[string]*pipelineNode)
	for nodeName, node := range p.Nodes {
		if node.OutDegree() != 0 && node.InDegree() != 0 {
			internals[nodeName] = node
		}
	}
	return internals
}

func (p *pipeline) sinks() []*pipelineNode {
	var sinks []*pipelineNode
	for _, node := range p.Nodes {
		if node.OutDegree() == 0 {
			sinks = append(sinks, node)
		}
	}
	return sinks
}

type pipelineNodeAPI interface {
	Init() error
	Close() error
}

type pipelineNode struct {
	inputChan     *chan interface{}
	outputChan    *chan interface{}
	value         pipelineNodeAPI
	children      []*pipelineNode
	parents       []*pipelineNode
	windowManager *windowManager
}

func (node *pipelineNode) Init() error {
	return node.value.Init()
}

func (node *pipelineNode) Close() {
	node.value.Close()
}

func (node *pipelineNode) InDegree() int {
	return len(node.parents)
}

func (node *pipelineNode) Children() []*pipelineNode {
	return node.children
}

func (node *pipelineNode) AddChild(child *pipelineNode) {
	node.children = append(node.children, child)
}

func (node *pipelineNode) Parents() []*pipelineNode {
	return node.parents
}

func (node *pipelineNode) AddParent(parent *pipelineNode) {
	node.parents = append(node.parents, parent)
}

func (node *pipelineNode) OutDegree() int {
	return len(node.children)
}

func makeSource(sourceConfig input.SourceConfig, sourceImpl input.SourceIface) (*pipelineNode, error) {
	sourceChan := make(chan interface{})
	source, err := sourceImpl.Create(sourceConfig)
	if err != nil {
		return nil, err
	}
	return &pipelineNode{
		outputChan: &sourceChan,
		value:      source,
	}, nil
}

func makeSink(sinkConfig output.SinkConfig, sinkImpl output.SinkIface) (*pipelineNode, error) {
	sinkChan := make(chan interface{})
	sink, err := sinkImpl.Create(sinkConfig)
	if err != nil {
		return nil, err
	}
	return &pipelineNode{
		inputChan: &sinkChan,
		value:     sink,
	}, nil
}

type pipelineManager struct {
	backendConfig
	Backend    backend
	sourceImpl input.SourceIface
	sinkImpl   output.SinkIface
}

func (pM *pipelineManager) Init() error {
	log.Debugln("Initialising Pipeline Manager")
	var err error
	pM.Backend, err = pM.backendConfig.Create()
	if err != nil {
		return err
	}

	if pM.sourceImpl == nil {
		pM.sourceImpl = &input.DefaultSource{}
	}
	if pM.sinkImpl == nil {
		pM.sinkImpl = &output.DefaultSink{}
	}
	return pM.Backend.Init()
}

func (pM *pipelineManager) Store(p *pipeline) error {
	return pM.Backend.Store(p)
}

func (pM *pipelineManager) Get(uuid []byte) ([]byte, error) {
	return pM.Backend.Get(uuid)
}

func (pM *pipelineManager) NewPipeline(rawConfig []byte) (*pipeline, error) {
	log.Debugln("Creating new pipeline")
	config, err := parseConfig(rawConfig)
	if err != nil {
		return nil, fmt.Errorf("Error parsing config %s", err)
	}
	if err := validateConfig(config); err != nil {
		return nil, fmt.Errorf("Error validating config %s", err)
	}

	pipe := &pipeline{
		ID:          uuid.New(),
		Config:      rawConfig,
		eventFolder: config.EventFolder,
		Nodes:       make(map[string]*pipelineNode),
	}

	for sourceName, sourceConfig := range config.Sources {
		source, err := makeSource(sourceConfig, pM.sourceImpl)
		if err != nil {
			return nil, fmt.Errorf("Error creating source %s", err)
		}
		pipe.addVertex(sourceName, source)
	}

	for sinkName, sinkConfig := range config.Sinks {
		sink, err := makeSink(sinkConfig, pM.sinkImpl)
		if err != nil {
			return nil, fmt.Errorf("Error creating sink %s", err)
		}
		pipe.addVertex(sinkName, sink)
	}

	for ruleName, ruleConfig := range config.Rules {
		var ruleState state.State
		var err error
		if ruleConfig.State != "" {
			ruleState, err = state.Create(config.States[ruleConfig.State])
			if err != nil {
				return nil, fmt.Errorf("Error creating rule state %s", err)
			}
		} else {
			ruleState = nil
		}

		rule, err := newRule(ruleConfig, ruleState)
		if err != nil {
			return nil, fmt.Errorf("Error creating rule %s", err)
		}
		ruleNode := &pipelineNode{
			value: rule,
		}
		pipe.addVertex(ruleName, ruleNode)
	}

	// Once all rules exist we can plumb them.
	// Doing so before this requires they be defined in the config in the order
	// that they are created in.
	for ruleName, ruleConfig := range config.Rules {
		ruleNode := pipe.Nodes[ruleName]
		pipe.addEdge(ruleNode, pipe.Nodes[ruleConfig.Sink])
		pipe.addEdge(pipe.Nodes[ruleConfig.Source], ruleNode)
	}

	err = pM.Store(pipe)
	if err != nil {
		return nil, fmt.Errorf("Error storing pipeline %s", err)
	}

	return pipe, nil
}

func (p *pipeline) StartPipeline() error {
	for _, sink := range p.sinks() {
		sVal, ok := sink.value.(output.Sink)
		if !ok {
			return fmt.Errorf("Expected %s to implement the Sink interface", sink.value)
		}
		err := output.StartOutput(sVal, sink.inputChan)
		if err != nil {
			return err
		}
	}

	for ruleName, rule := range p.internals() {
		log.Infof("Starting rule %s", ruleName)
		outputChan := make(chan interface{})
		rule.outputChan = &outputChan
		rVal := rule.value.(Rule)
		(*rule).windowManager = &windowManager{
			sinkChan: rule.outputChan,
			rule:     rVal,
		}
		(*rule).inputChan = startRule(rVal, rule.outputChan, rule.windowManager)
		for _, child := range rule.Children() {
			go func(sink *pipelineNode, source *pipelineNode) {
				for evt := range *source.outputChan {
					*sink.inputChan <- evt
				}
			}(child, rule)
		}
	}

	eventTypes, err := getEventTypes(p.eventFolder)
	if err != nil {
		log.Fatalf("Failed to get Event plugins: %v", err)
	}

	for _, source := range p.sources() {
		sVal := source.value.(input.Source)
		err := input.StartInput(sVal, source.outputChan)
		if err != nil {
			return err
		}
		go func(source *pipelineNode) {
			for data := range *source.outputChan {
				evt, err := matchEventType(eventTypes, data)
				if err != nil {
					log.Infof("Error matching event: %v %v", err, data)
				}
				for _, node := range source.Children() {
					*node.inputChan <- evt
				}
			}
		}(source)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	for sig := range c {
		log.Infof("Received %s signal... exiting\n", sig)
		break
	}

	p.Close()
	return nil
}

func (p *pipeline) Close() {
	log.Debug("Closing input channels\n")
	for _, s := range p.sources() {
		s.Close()
	}

	log.Debug("Closing rule channels\n")
	for _, i := range p.internals() {
		i.windowManager.stop()
		close(*i.outputChan)
		i.Close()
	}

	log.Debug("Closing output channels\n")
	for _, o := range p.sinks() {
		close(*o.inputChan)
		o.Close()
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
