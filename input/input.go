package input

import (
	"fmt"
)

// Input is an interface for input implemenations
type Source interface {
	Retrieve(*chan []byte)
	Init() error
}

type SourceConfig struct {
	Type          string        `json:"type"`
	FileConfig    FileConfig    `json:"file_config,omitempty"`
	KinesisConfig KinesisConfig `json:"kinesis_config,omitempty"`
	KafkaConfig   KafkaConfig   `json:"kafka_config,omitempty"`
}

func Create(config SourceConfig) (Source, error) {
	switch config.Type {
	case "Kinesis":
		return &KinesisInput{
			StreamName: config.KinesisConfig.StreamName,
		}, nil
	case "Kafka":
		return &KafkaInput{
			Broker:     config.KafkaConfig.Broker,
			Topic:      config.KafkaConfig.Topic,
			Partitions: config.KafkaConfig.Partitions,
		}, nil
	case "File":
		return &FileInput{FileName: config.FileConfig.Path}, nil
	case "CertStream":
		return &CertStreamInput{}, nil
	}
	return nil, fmt.Errorf("Invalid input type: %v", config.Type)
}

func StartInput(in *Source, inChan *chan []byte) error {
	err := (*in).Init()
	if err != nil {
		return fmt.Errorf("Input setup failed: %v", err)
	}
	go (*in).Retrieve(inChan)
	return nil
}
