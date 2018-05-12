// +build integration

package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/patrobinson/go-fish/input"
	"github.com/patrobinson/go-fish/output"
	"github.com/patrobinson/go-fish/state"
)

type testInput struct {
	value string
}

func (t testInput) Init() error {
	return nil
}

func (t *testInput) Retrieve(out *chan []byte) {
	defer close(*out)
	*out <- []byte(t.value)
}

type testOutput struct {
	c *chan bool
}

func (t testOutput) Init() error {
	return nil
}

func (t *testOutput) Sink(in *chan interface{}, wg *sync.WaitGroup) {
	defer (*wg).Done()
	for msg := range *in {
		log.Info("testOutput: Output received")
		*t.c <- msg.(bool)
	}
	log.Info("testOutput: Output closed")
}

func setupBasicPipeline(output *chan bool, input string) (*Pipeline, error) {
	aRule, err := NewRule(ruleConfig{
		Source: "testInput",
		Plugin: "testdata/rules/a.so",
		Sink:   "testOutput",
	}, nil)
	if err != nil {
		return nil, err
	}

	lengthRule, err := NewRule(ruleConfig{
		Source: "testInput",
		Plugin: "testdata/rules/length.so",
		Sink:   "testOutput",
	}, nil)
	if err != nil {
		return nil, err
	}

	testInput := &testInput{value: input}
	testOutput := &testOutput{c: output}

	outChan := make(chan interface{})
	testSinkMapper := &SinkMapper{
		SinkChannel: &outChan,
		Sink:        testOutput,
	}

	aRuleMapper := &RuleMapper{
		Rule: aRule,
		Sink: testSinkMapper,
	}
	bRuleMapper := &RuleMapper{
		Rule: lengthRule,
		Sink: testSinkMapper,
	}
	ruleMappings := map[string]*RuleMapper{
		"a":      aRuleMapper,
		"length": bRuleMapper,
	}

	return setupPipeline(ruleMappings, testInput, testSinkMapper, "testdata/eventTypes")
}

func setupPipeline(ruleMappings map[string]*RuleMapper, testInput input.Source, testSinkMapper *SinkMapper, eventFolder string) (*Pipeline, error) {
	inChan := make(chan []byte)

	var ruleMapSlice []*RuleMapper
	for _, ruleMap := range ruleMappings {
		ruleMapSlice = append(ruleMapSlice, ruleMap)
	}

	pipeline := Pipeline{
		Rules: ruleMappings,
		Sources: map[string]*SourceMapper{
			"testInput": &SourceMapper{
				SourceChannel: &inChan,
				Source:        testInput,
				Rules:         ruleMapSlice,
			},
		},
		Sinks: map[string]*SinkMapper{
			"testOutput": testSinkMapper,
		},
		eventFolder: eventFolder,
	}

	pipeline.StartPipeline()
	return &pipeline, nil
}

func TestSuccessfulRun(t *testing.T) {
	output := make(chan bool)
	pipeline, err := setupBasicPipeline(&output, "a")
	if err != nil {
		t.Errorf("Error creating pipeline: %s", err)
	}
	defer pipeline.Close()

	go pipeline.Run()
	r1 := <-output
	fmt.Print("Received 1 output\n")
	r2 := <-output
	fmt.Print("Received 2 output\n")
	if !r1 || !r2 {
		t.Errorf("Rules did not match %v %v", r1, r2)
	}
}

func TestFailRun(t *testing.T) {
	output := make(chan bool)
	pipeline, err := setupBasicPipeline(&output, "abc")
	if err != nil {
		t.Errorf("Error creating pipeline: %s", err)
	}
	defer pipeline.Close()

	go pipeline.Run()
	if r1, r2 := <-output, <-output; r1 || r2 {
		t.Errorf("Rules did not match %v %v", r1, r2)
	}
}

type benchmarkInput struct {
	input *chan []byte
}

func (t benchmarkInput) Init() error {
	return nil
}

func (t *benchmarkInput) Retrieve(out *chan []byte) {
	defer close(*out)
	for in := range *t.input {
		*out <- in
	}
}

func TestStreamToStreamStateIntegration(t *testing.T) {
	defer os.Remove("assumeRoleEnrichment")
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	expectedEvent := output.OutputEvent{
		Source:    "CloudTrail",
		EventTime: time.Date(2016, 11, 14, 17, 25, 45, 0, &time.Location{}).UTC(),
		EventType: "UserCreated",
		Name:      "IAMUserCreated",
		Level:     output.WarnLevel,
		EventId:   "dEXAMPLE-265a-41e0-9352-4401bEXAMPLE",
		Entity:    "user/Bob",
		SourceIP:  "192.0.2.1",
		Body: map[string]interface{}{
			"AccountID":   "777788889999",
			"UserCreated": "god_user",
		},
		Occurrences: 1,
	}

	outChan := make(chan interface{})
	out := &testStatefulOutput{c: &outChan}
	inChan := make(chan []byte)
	in := &testStatefulInput{
		channel: &inChan,
		inputs:  2,
	}
	outSink := make(chan interface{})
	testSinkMapper := &SinkMapper{
		SinkChannel: &outSink,
		Sink:        out,
	}
	ruleState := &state.KVStore{
		DbFileName: "s2s.db",
		BucketName: "s2s",
	}
	err := ruleState.Init()
	if err != nil {
		t.Errorf("Error starting state: %s", err)
	}
	s2sRule, err := NewRule(ruleConfig{
		Plugin: "testdata/statefulIntegrationTests/s2s_rules/cloudTrail_s2s_join.so",
	}, ruleState)
	if err != nil {
		t.Errorf("Error while creating s2s rule plugin: %s", err)
	}

	s2sRuleMapping := &RuleMapper{
		Rule: s2sRule,
		Sink: testSinkMapper,
	}
	ruleMappings := map[string]*RuleMapper{
		"s2s": s2sRuleMapping,
	}
	pipeline, err := setupPipeline(ruleMappings, in, testSinkMapper, "testdata/statefulIntegrationTests/eventTypes")
	if err != nil {
		t.Errorf("Error while setting up pipeline: %v", err)
	}
	defer pipeline.Close()

	go pipeline.Run()

	assumeRoleEvent, _ := ioutil.ReadFile("testdata/statefulIntegrationTests/assumeRoleEvent.json")
	inChan <- assumeRoleEvent

	r2 := <-outChan
	fmt.Print("Received 1 output\n")

	createUserEvent, _ := ioutil.ReadFile("testdata/statefulIntegrationTests/createUserEvent.json")
	inChan <- createUserEvent

	r2 = <-outChan
	fmt.Print("Received 2 output\n")
	testOutput, ok := r2.(output.OutputEvent)
	if !ok {
		t.Errorf("Expected output to be an OutputEvent, got: %v", r2)
	}
	if !reflect.DeepEqual(testOutput, expectedEvent) {
		t.Errorf("Expected %v\nGot %v\n", expectedEvent, r2)
		fmt.Printf("Source: %v\n", testOutput.Source == expectedEvent.Source)
		fmt.Printf("EventTime: %v\n", testOutput.EventTime == expectedEvent.EventTime)
		fmt.Printf("EventType: %v\n", testOutput.EventType == expectedEvent.EventType)
		fmt.Printf("Name: %v\n", testOutput.Name == expectedEvent.Name)
		fmt.Printf("Level: %v\n", testOutput.Level == expectedEvent.Level)
		fmt.Printf("EventId: %v\n", testOutput.EventId == expectedEvent.EventId)
		fmt.Printf("Entity: %v\n", testOutput.Entity == expectedEvent.Entity)
		fmt.Printf("SourceIP: %v\n", testOutput.EventId == expectedEvent.SourceIP)
		fmt.Printf("Body: %v\n", reflect.DeepEqual(testOutput.Body, expectedEvent.Body))
	}
}

type testStatefulInput struct {
	channel *chan []byte
	inputs  int
}

func (t testStatefulInput) Init() error {
	return nil
}

func (t *testStatefulInput) Retrieve(out *chan []byte) {
	defer close(*out)
	for i := 0; i < t.inputs; i++ {
		output := <-*t.channel
		*out <- output
	}
}

type testStatefulOutput struct {
	c *chan interface{}
}

func (t testStatefulOutput) Init() error {
	return nil
}

func (t *testStatefulOutput) Sink(in *chan interface{}, wg *sync.WaitGroup) {
	defer (*wg).Done()
	for msg := range *in {
		log.Info("testStatefulOutput: Output received")
		*t.c <- msg
	}
	log.Info("testStatefulOutput: Output closed")
}

func TestAggregateStateIntegration(t *testing.T) {
	defer os.Remove("aggregateEvent")
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	expectedEvent := output.OutputEvent{
		Source:    "CloudTrail",
		EventTime: time.Date(2016, 11, 14, 17, 25, 45, 0, &time.Location{}).UTC(),
		EventType: "NoMFA",
		Name:      "NoMFA",
		Level:     output.WarnLevel,
		EventId:   "dEXAMPLE-265a-41e0-9352-4401bEXAMPLE",
		Entity:    "role/AssumeNothing",
		SourceIP:  "192.0.2.1",
		Body: map[string]interface{}{
			"AccountID": "777788889999",
		},
		Occurrences: 3,
	}

	outChan := make(chan interface{})
	out := &testStatefulOutput{c: &outChan}
	inChan := make(chan []byte)
	in := &testStatefulInput{
		channel: &inChan,
		inputs:  4,
	}
	outSink := make(chan interface{})
	testSinkMapper := &SinkMapper{
		SinkChannel: &outSink,
		Sink:        out,
	}
	ruleState := &state.KVStore{
		DbFileName: "agg.db",
		BucketName: "agg",
	}
	err := ruleState.Init()
	if err != nil {
		t.Errorf("Error starting state: %s", err)
	}
	aggRule, err := NewRule(ruleConfig{
		Plugin: "testdata/statefulIntegrationTests/agg_rules/cloudTrail_agg.so",
	}, ruleState)
	if err != nil {
		t.Errorf("Error while creating agg rule plugin: %s", err)
	}

	aggRuleMapping := &RuleMapper{
		Rule: aggRule,
		Sink: testSinkMapper,
	}
	ruleMappings := map[string]*RuleMapper{
		"agg": aggRuleMapping,
	}
	pipeline, err := setupPipeline(ruleMappings, in, testSinkMapper, "testdata/statefulIntegrationTests/eventTypes")
	if err != nil {
		t.Errorf("Error while setting up pipeline: %v", err)
	}
	defer pipeline.Close()

	go pipeline.Run()

	createUserEvent, _ := ioutil.ReadFile("testdata/statefulIntegrationTests/createUserEvent.json")
	inChan <- createUserEvent
	outEvent := <-outChan
	if outEvent != nil {
		t.Errorf("Expected %v\nGot %v\n", nil, out)
	}

	inChan <- createUserEvent
	outEvent = <-outChan
	if outEvent != nil {
		t.Errorf("Expected %v\nGot %v\n", nil, out)
	}

	inChan <- createUserEvent
	outEvent = <-outChan
	if outEvent != nil {
		t.Errorf("Expected %v\nGot %v\n", nil, out)
	}

	outEvent = <-outChan

	if !reflect.DeepEqual(outEvent.(output.OutputEvent), expectedEvent) {
		t.Errorf("Expected %v\nGot %v\n", expectedEvent, out)
		event := outEvent.(output.OutputEvent)
		fmt.Printf("Source: %v\n", event.Source == expectedEvent.Source)
		fmt.Printf("EventTime: %v\n", event.EventTime == expectedEvent.EventTime)
		fmt.Printf("EventType: %v\n", event.EventType == expectedEvent.EventType)
		fmt.Printf("Name: %v\n", event.Name == expectedEvent.Name)
		fmt.Printf("Level: %v\n", event.Level == expectedEvent.Level)
		fmt.Printf("EventId: %v\n", event.EventId == expectedEvent.EventId)
		fmt.Printf("Entity: %v\n", event.Entity == expectedEvent.Entity)
		fmt.Printf("SourceIP: %v\n", event.EventId == expectedEvent.SourceIP)
		fmt.Printf("Body: %v\n", reflect.DeepEqual(event.Body, expectedEvent.Body))
	}
}
