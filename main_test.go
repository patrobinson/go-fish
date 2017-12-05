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
	"github.com/patrobinson/go-fish/output"
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
		log.Info("Input received")
		*t.c <- msg.(bool)
	}
	log.Info("Input closed")
}

func TestSuccessfulRun(t *testing.T) {
	output := make(chan bool)
	//out := &testOutput{c: &output}
	//in := &testInput{value: "a"}
	pipeline := Pipeline{}
	pipeline.Run()
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
	//out := &testOutput{c: &output}
	//in := &testInput{value: "abc"}
	pipeline := Pipeline{}
	pipeline.Run()
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

func BenchmarkRun(b *testing.B) {
	log.SetLevel(log.WarnLevel)
	output := make(chan bool)
	//out := &testOutput{c: &output}

	input := make(chan []byte)
	//in := &benchmarkInput{input: &input}
	var r1 bool
	var r2 bool
	b.ResetTimer()
	//go run("testdata/rules", "testdata/eventTypes", in, out)
	pipeline := Pipeline{}
	pipeline.Run()
	for i := 0; i < b.N; i++ {
		bs := make([]byte, 1)
		bs[0] = byte(i)
		input <- bs
		r1 = <-output
		r2 = <-output
	}
	r := r1 || r2
	fmt.Printf("%v\n", r)
}

// +build integration

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
	//out := &testStatefulOutput{c: &outChan}
	inChan := make(chan []byte)
	/*in := &testStatefulInput{
		channel: &inChan,
		inputs:  2,
	}*/
	//go run("testdata/statefulIntegrationTests/s2s_rules", "testdata/statefulIntegrationTests/eventTypes", in, out)
	pipeline := Pipeline{}
	pipeline.Run()

	assumeRoleEvent, _ := ioutil.ReadFile("testdata/statefulIntegrationTests/assumeRoleEvent.json")
	inChan <- assumeRoleEvent

	r2 := <-outChan
	fmt.Print("Received 1 output\n")

	createUserEvent, _ := ioutil.ReadFile("testdata/statefulIntegrationTests/createUserEvent.json")
	inChan <- createUserEvent

	r2 = <-outChan
	fmt.Print("Received 2 output\n")
	if !reflect.DeepEqual(r2.(output.OutputEvent), expectedEvent) {
		t.Errorf("Expected %v\nGot %v\n", expectedEvent, r2)
		event := r2.(output.OutputEvent)
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
		log.Info("Input received")
		*t.c <- msg
	}
	log.Info("Input closed")
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
	//outProcessor := &testStatefulOutput{c: &outChan}
	inChan := make(chan []byte)
	/*in := &testStatefulInput{
		channel: &inChan,
		inputs:  4,
	}*/
	//go run("testdata/statefulIntegrationTests/agg_rules", "testdata/statefulIntegrationTests/eventTypes", in, outProcessor)
	pipeline := Pipeline{}
	pipeline.Run()

	createUserEvent, _ := ioutil.ReadFile("testdata/statefulIntegrationTests/createUserEvent.json")
	inChan <- createUserEvent
	out := <-outChan
	if out != nil {
		t.Errorf("Expected %v\nGot %v\n", nil, out)
	}

	inChan <- createUserEvent
	out = <-outChan
	if out != nil {
		t.Errorf("Expected %v\nGot %v\n", nil, out)
	}

	inChan <- createUserEvent
	out = <-outChan
	if out != nil {
		t.Errorf("Expected %v\nGot %v\n", nil, out)
	}

	out = <-outChan

	if !reflect.DeepEqual(out.(output.OutputEvent), expectedEvent) {
		t.Errorf("Expected %v\nGot %v\n", expectedEvent, out)
		event := out.(output.OutputEvent)
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
