// +build integration

package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/patrobinson/go-fish/input"
	"github.com/patrobinson/go-fish/output"
	log "github.com/sirupsen/logrus"
)

type testInput struct {
	value []byte
}

func (t testInput) Init() error {
	return nil
}

func (t *testInput) Retrieve(out *chan interface{}) {
	defer close(*out)
	*out <- interface{}(t.value)
}

func (t *testInput) Close() error {
	return nil
}

type testOutput struct {
	c *chan bool
}

func (t testOutput) Init() error {
	return nil
}

func (t *testOutput) Sink(in *chan interface{}) {
	for msg := range *in {
		log.Infoln("testOutput: Output received", msg)
		*t.c <- msg.(bool)
	}
	log.Info("testOutput: Output closed")
}

func (t *testOutput) Close() error {
	return nil
}

type testSink struct {
	sink output.Sink
}

func (t *testSink) Create(config output.SinkConfig) (output.Sink, error) {
	return t.sink, nil
}

func (t *testSink) Close() error {
	return nil
}

type testSource struct {
	source input.Source
}

func (t *testSource) Create(config input.SourceConfig) (input.Source, error) {
	return t.source, nil
}

func setupPipeline(source input.Source, sink output.Sink, config []byte, databaseName string) (*pipeline, error) {
	sourceImpl := &testSource{source: source}
	sinkImpl := &testSink{sink: sink}

	pManager := pipelineManager{
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
	err := pManager.Init()
	if err != nil {
		return nil, err
	}
	p, err := pManager.NewPipeline(config)
	if err != nil {
		return nil, err
	}

	go func() {
		err = p.StartPipeline()
		if err != nil {
			panic(err)
		}
	}()
	return p, nil
}

func setupBasicPipeline(output *chan bool, input []byte, databaseName string) (*pipeline, error) {
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
	_, err := setupBasicPipeline(&output, []byte("a"), "successfulRun.db")
	if err != nil {
		t.Fatalf("Error creating pipeline: %s", err)
	}
	r1 := <-output
	t.Log("Received 1 output\n")
	r2 := <-output
	t.Log("Received 2 output\n")
	if !r1 || !r2 {
		t.Errorf("Rules did not match %v %v", r1, r2)
	}
}

func TestFailRun(t *testing.T) {
	output := make(chan bool)
	_, err := setupBasicPipeline(&output, []byte("abc"), "failRun.db")
	if err != nil {
		t.Fatalf("Error creating pipeline: %s", err)
	}
	if r1, r2 := <-output, <-output; r1 || r2 {
		t.Errorf("Rules did not match %v %v", r1, r2)
	}
}

type benchmarkInput struct {
	input *chan interface{}
}

func (t benchmarkInput) Init() error {
	return nil
}

func (t *benchmarkInput) Retrieve(out *chan interface{}) {
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
	inChan := make(chan interface{})
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

	_, err := setupPipeline(in, out, config, "streamToStream.db")
	if err != nil {
		t.Fatalf("Error while setting up pipeline: %v", err)
	}

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
	channel *chan interface{}
	inputs  int
}

func (t *testStatefulInput) Init() error {
	return nil
}

func (t *testStatefulInput) Retrieve(out *chan interface{}) {
	defer close(*out)
	for i := 0; i < t.inputs; i++ {
		output := <-*t.channel
		*out <- output
	}
}

func (t *testStatefulInput) Close() error {
	return nil
}

type testStatefulOutput struct {
	c *chan interface{}
}

func (t *testStatefulOutput) Init() error {
	return nil
}

func (t *testStatefulOutput) Sink(in *chan interface{}) {
	for msg := range *in {
		log.Info("testStatefulOutput: Output received")
		*t.c <- msg
	}
	log.Info("testStatefulOutput: Output closed")
}

func (t *testStatefulOutput) Close() error {
	return nil
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
	inChan := make(chan interface{})
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

	_, err := setupPipeline(in, out, config, "agg.db")
	if err != nil {
		t.Fatalf("Error while setting up pipeline: %v", err)
	}

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
