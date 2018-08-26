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

	"github.com/patrobinson/go-fish/input"
	"github.com/patrobinson/go-fish/output"
	log "github.com/sirupsen/logrus"
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

type testSink struct {
	sink output.Sink
}

func (t *testSink) Create(config output.SinkConfig) (output.Sink, error) {
	return t.sink, nil
}

type testSource struct {
	source input.Source
}

func (t *testSource) Create(config input.SourceConfig) (input.Source, error) {
	return t.source, nil
}

func setupPipeline(source input.Source, sink output.Sink, config []byte, databaseName string) (*Pipeline, error) {
	sourceImpl := &testSource{source: source}
	sinkImpl := &testSink{sink: sink}

	pipelineManager := PipelineManager{
		backendConfig: backendConfig{
			Type: "boltdb",
			BoltDBConfig: boltDBConfig{
				BucketName:   "gofish",
				DatabaseName: databaseName,
			},
		},
		sourceImpl: sourceImpl,
		sinkImpl:   sinkImpl,
	}
	err := pipelineManager.Init()
	if err != nil {
		return nil, err
	}
	pipeline, err := pipelineManager.NewPipeline(config)
	if err != nil {
		return nil, err
	}
	err = pipeline.StartPipeline()
	if err != nil {
		return nil, err
	}
	return pipeline, nil
}

func setupBasicPipeline(output *chan bool, input string, databaseName string) (*Pipeline, error) {
	testInput := &testInput{value: input}
	testOutput := &testOutput{c: output}
	config := []byte(`{
		"eventFolder": "testdata/eventTypes",
		"rules": {
			"aRule": {
				"source": "testInput",
				"plugin": "testdata/rules/a.so",
				"sink": "testOutput"
			},
			"lengthRule": {
				"source": "testInput",
				"plugin": "testdata/rules/length.so",
				"sink": "testOutput"
			}
		},
		"sources": {
			"testInput": {
				"type": "test"
			}
		},
		"sinks": {
			"testOutput": {
				"type": "test"
			}
		}
	}`)
	return setupPipeline(testInput, testOutput, config, databaseName)
}

func TestSuccessfulRun(t *testing.T) {
	output := make(chan bool)
	pipeline, err := setupBasicPipeline(&output, "a", "successfulRun.db")
	if err != nil {
		t.Fatalf("Error creating pipeline: %s", err)
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
	pipeline, err := setupBasicPipeline(&output, "abc", "failRun.db")
	if err != nil {
		t.Fatalf("Error creating pipeline: %s", err)
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
	log.SetLevel(log.DebugLevel)
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
	config := []byte(`{
		"eventFolder": "testdata/statefulIntegrationTests/eventTypes",
		"rules": {
			"s2sRule": {
				"source": "testStatefulInput",
				"plugin": "testdata/statefulIntegrationTests/s2s_rules/cloudTrail_s2s_join.so",
				"state": "kv",
				"sink": "testStatefulOutput"
			}
		},
		"states" : {
			"kv": {
				"type": "KV",
				"kvConfig": {
					"dbFileName": "testdata/s2s.db",
					"bucketName": "s2s"
				}
			}
		},
		"sources": {
			"testStatefulInput": {
				"type": "test"
			}
		},
		"sinks": {
			"testStatefulOutput": {
				"type": "test"
			}
		}
	}`)

	pipeline, err := setupPipeline(in, out, config, "streamToStream.db")
	if err != nil {
		t.Fatalf("Error while setting up pipeline: %v", err)
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
	config := []byte(`{
		"eventFolder": "testdata/statefulIntegrationTests/eventTypes",
		"rules": {
			"aggRule": {
				"source": "testStatefulInput",
				"plugin": "testdata/statefulIntegrationTests/agg_rules/cloudTrail_agg.so",
				"state": "kv",
				"sink": "testStatefulOutput"
			}
		},
		"states" : {
			"kv": {
				"type": "KV",
				"kvConfig": {
					"dbFileName": "testdata/agg.db",
					"bucketName": "agg"
				}
			}
		},
		"sources": {
			"testStatefulInput": {
				"type": "test"
			}
		},
		"sinks": {
			"testStatefulOutput": {
				"type": "test"
			}
		}
	}`)

	pipeline, err := setupPipeline(in, out, config, "agg.db")
	if err != nil {
		t.Fatalf("Error while setting up pipeline: %v", err)
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
