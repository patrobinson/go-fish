package main

import (
	"reflect"
	"strings"
	"testing"
)

func Test_parseConfig(t *testing.T) {
	configFile := strings.NewReader(
		`{
    "input": "Kinesis",
    "kinesisConfig": {
        "streamName": "GoFish"
    },
    "fileConfig": {
        "outputFile": "/tmp/out"
    },
    "RuleFolder": "/tmp/rules",
    "EventTypeFolder": "/tmp/eventTypes"
}`)

	want := config{
		Input: "Kinesis",
		KinesisConfig: &kinesisConfig{
			StreamName: "GoFish",
		},
		FileConfig: &fileConfig{
			OutputFile: "/tmp/out",
		},
		RuleFolder:      "/tmp/rules",
		EventTypeFolder: "/tmp/eventTypes",
	}
	got, err := parseConfig(configFile)
	if err != nil {
		t.Errorf("parseConfig() error = %v", err)
		return
	}
	if got.Input != want.Input || got.RuleFolder != want.RuleFolder || got.EventTypeFolder != want.EventTypeFolder {
		t.Errorf("parseConfig() = %v, want %v", got, want)
	}
	if !reflect.DeepEqual(*got.KinesisConfig, *want.KinesisConfig) {
		t.Errorf("parseConfig() = %v, want %v", *got.KinesisConfig, *want.KinesisConfig)
	}
	if !reflect.DeepEqual(*got.FileConfig, *want.FileConfig) {
		t.Errorf("parseConfig() = %v, want %v", *got.FileConfig, *want.FileConfig)
	}
}
