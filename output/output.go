package output

import (
	"fmt"
)

type SinkConfig struct {
	Type       string     `json:"type"`
	FileConfig FileConfig `json:"file_config,omitempty"`
	SqsConfig  SqsConfig  `json:"sqs_config,omitempty"`
}

// Sink is an interface for output implementations
type Sink interface {
	Sink(*chan interface{})
	Init() error
	Close() error
}

// SourceIface provides an interface for creating input sources
type SinkIface interface {
	Create(config SinkConfig) (Sink, error)
}

// DefaultSource is an implementation of the SourceIface used to create inputs
type DefaultSink struct{}

func (*DefaultSink) Create(config SinkConfig) (Sink, error) {
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

func StartOutput(out Sink, outChannel *chan interface{}) error {
	err := out.Init()
	if err != nil {
		return fmt.Errorf("Output setup failed: %v", err)
	}
	go out.Sink(outChannel)
	return nil
}
