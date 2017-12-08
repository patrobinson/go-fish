package main

import (
	"os"
	"reflect"
	"testing"

	"github.com/patrobinson/go-fish/input"
	"github.com/patrobinson/go-fish/output"
)

func TestParseConfig(t *testing.T) {
	testdata, _ := os.Open("testdata/pipelines/config.json")
	expectedConfig := PipelineConfig{
		EventFolder: "testdata/eventTypes",
		Rules: map[string]ruleConfig{
			"searchRule": ruleConfig{
				Source: "fileInput",
				State:  "searchConversion",
				Plugin: "testdata/rules/search_rule.so",
				Sink:   "fileOutput",
			},
			"conversionRule": ruleConfig{
				Source: "fileInput",
				State:  "searchConversion",
				Plugin: "testdata/rules/conversion_rule.so",
			},
		},
		States: map[string]stateConfig{
			"searchConversion": stateConfig{
				Type: "kv",
			},
		},
		Sources: map[string]input.SourceConfig{
			"fileInput": input.SourceConfig{
				Type: "file",
				FileConfig: input.FileConfig{
					Path: "testdata/pipelines/input/file.in",
				},
			},
		},
		Sinks: map[string]output.SinkConfig{
			"fileOutput": output.SinkConfig{
				Type: "file",
				FileConfig: output.FileConfig{
					Path: "testdata/pipelines/output/file.out",
				},
			},
		},
	}

	parsedConfig, err := parseConfig(testdata)
	if err != nil {
		t.Errorf("Expected error to be nil, got %s", err)
	}
	if !reflect.DeepEqual(parsedConfig, expectedConfig) {
		t.Errorf("Expected config:\n%v\nGot config:\n%v", expectedConfig, parsedConfig)
	}
}
