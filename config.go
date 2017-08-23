package main

import (
	"encoding/json"
	"io"
)

type config struct {
	Input           string         `json:"input"`
	KafkaConfig     *kafkaConfig   `json:"kafkaConfig,omitempty"`
	KinesisConfig   *kinesisConfig `json:"kinesisConfig,omitempty"`
	FileConfig      *fileConfig    `json:"fileConfig"`
	RuleFolder      string
	EventTypeFolder string
}

type kafkaConfig struct {
	Broker     string `json:"broker"`
	Topic      string `json:"topic"`
	Partitions int32  `json:"partitions"`
}

type kinesisConfig struct {
	StreamName string `json:"streamName"`
}

type fileConfig struct {
	InputFile  string `json:"inputFile,omitempty"`
	OutputFile string `json:"outputFile"`
}

func parseConfig(configFile io.Reader) (config, error) {
	var config config
	jsonParser := json.NewDecoder(configFile)
	err := jsonParser.Decode(&config)
	return config, err
}
