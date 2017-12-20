package output

import (
	"fmt"
	"sync"
)

type SinkConfig struct {
	Type       string     `json:"type"`
	FileConfig FileConfig `json:"file_config,omitempty"`
	SqsConfig  SqsConfig  `json:"sqs_config,omitempty"`
}

// Sink is an interface for output implementations
type Sink interface {
	Sink(*chan interface{}, *sync.WaitGroup)
	Init() error
}

func Create(config SinkConfig) (Sink, error) {
	switch config.Type {
	case "SQS":
		return &SQSOutput{
			QueueUrl: config.SqsConfig.QueueUrl,
			Region:   config.SqsConfig.Region,
		}, nil
	case "File":
		return &FileOutput{
			FileName: config.FileConfig.Path,
		}, nil
	}

	return nil, fmt.Errorf("Invalid output type: %v", config.Type)
}

func StartOutput(out *Sink, wg *sync.WaitGroup, outChannel *chan interface{}) error {
	err := (*out).Init()
	if err != nil {
		return fmt.Errorf("Output setup failed: %v", err)
	}
	(*wg).Add(1)
	go (*out).Sink(outChannel, wg)
	return nil
}
