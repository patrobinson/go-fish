package main

import (
	"os"
	"sync"

	log "github.com/Sirupsen/logrus"
	"github.com/patrobinson/go-fish/input"
	"github.com/patrobinson/go-fish/output"
)

// Input is an interface for input implemenations
type Input interface {
	Retrieve(*chan []byte)
	Init() error
}

// Output is an interface for output implementations
type Output interface {
	Sink(*chan interface{}, *sync.WaitGroup)
	Init() error
}

func main() {
	configFile := os.Args[1]
	file, err := os.Open(configFile)
	if err != nil {
		log.Fatalf("Failed to open Config File: %v", err)
	}
	config, err := parseConfig(file)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	var in interface{}
	if config.Input == "Kinesis" {
		in = &input.KinesisInput{
			StreamName: (*config.KinesisConfig).StreamName,
		}
	} else if config.Input == "File" {
		in = &input.FileInput{FileName: (*config.FileConfig).InputFile}
	} else {
		log.Fatalf("Invalid input type: %v", config.Input)
	}

	var out interface{}
	if config.Output == "SQS" {
		out = &output.SQSOutput{
			QueueUrl: config.SqsConfig.QueueUrl,
			Region:   config.SqsConfig.Region,
		}
	} else if config.Output == "File" {
		out = &output.FileOutput{
			FileName: (*config.FileConfig).OutputFile,
		}
	} else {
		log.Fatalf("Invalid output type: %v", config.Output)
	}

	run(config.RuleFolder, config.EventTypeFolder, in, out)
}

func run(rulesFolder string, eventFolder string, in interface{}, out interface{}) {
	input := in.(Input)
	output := out.(Output)

	var outWg sync.WaitGroup
	var ruleWg sync.WaitGroup

	outChan := startOutput(&output, &outWg)
	rChans := startRules(rulesFolder, outChan, &ruleWg)
	inChan := startInput(&input)
	eventTypes, err := getEventTypes(eventFolder)
	if err != nil {
		log.Fatalf("Failed to get Event plugins: %v", err)
	}

	// receive from inputs and send to all rules
	func(iChan *chan []byte, ruleChans []*chan interface{}) {
		for data := range *iChan {
			evt, err := matchEventType(eventTypes, data)
			if err != nil {
				log.Infof("Error matching event: %v", err)
			}
			for _, i := range ruleChans {
				*i <- evt
			}
		}
	}(inChan, rChans)

	log.Debug("Input done, closing rule channels\n")

	for _, c := range rChans {
		close(*c)
	}
	ruleWg.Wait()

	log.Debug("Closing output channels\n")
	close(*outChan)
	outWg.Wait()
}

func startOutput(out *Output, wg *sync.WaitGroup) *chan interface{} {
	err := (*out).Init()
	if err != nil {
		log.Fatalf("Input setup failed: %v", err)
	}
	(*wg).Add(1)
	outChan := make(chan interface{})
	go (*out).Sink(&outChan, wg)
	return &outChan
}

func startInput(in *Input) *chan []byte {
	err := (*in).Init()
	if err != nil {
		log.Fatalf("Input setup failed: %v", err)
	}
	inChan := make(chan []byte)
	go (*in).Retrieve(&inChan)
	return &inChan
}
