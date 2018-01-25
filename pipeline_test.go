package main

import (
	"os"
	"reflect"
	"testing"

	"github.com/patrobinson/go-fish/input"
	"github.com/patrobinson/go-fish/output"
	"github.com/patrobinson/go-fish/state"
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
		States: map[string]state.StateConfig{
			"searchConversion": state.StateConfig{
				Type: "kv",
			},
		},
		Sources: map[string]input.SourceConfig{
			"fileInput": input.SourceConfig{
				Type: "File",
				FileConfig: input.FileConfig{
					Path: "testdata/pipelines/input/file.in",
				},
			},
		},
		Sinks: map[string]output.SinkConfig{
			"fileOutput": output.SinkConfig{
				Type: "File",
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

func TestNewPipeline(t *testing.T) {
	pipelineConfig := PipelineConfig{
		EventFolder: "testdata/eventTypes",
		Rules: map[string]ruleConfig{
			"aRule": ruleConfig{
				Source: "fileInput",
				Plugin: "testdata/rules/a.so",
				Sink:   "fileOutput",
			},
			"lengthRule": ruleConfig{
				Source: "fileInput",
				Plugin: "testdata/rules/length.so",
				Sink:   "fileOutput",
			},
		},
		Sources: map[string]input.SourceConfig{
			"fileInput": input.SourceConfig{
				Type: "File",
				FileConfig: input.FileConfig{
					Path: "testdata/pipelines/input",
				},
			},
		},
		Sinks: map[string]output.SinkConfig{
			"fileOutput": output.SinkConfig{
				Type: "File",
				FileConfig: output.FileConfig{
					Path: "testdata/pipelines/output",
				},
			},
		},
	}
	_, err := NewPipeline(pipelineConfig)
	if err != nil {
		t.Errorf("Error creating new pipeline: %s", err)
	}
}

func TestStartPipeline(t *testing.T) {
	pipelineConfig := PipelineConfig{
		EventFolder: "testdata/eventTypes",
		Rules: map[string]ruleConfig{
			"aRule": ruleConfig{
				Source: "fileInput",
				Plugin: "testdata/rules/a.so",
				Sink:   "fileOutput",
			},
			"lengthRule": ruleConfig{
				Source: "fileInput",
				Plugin: "testdata/rules/length.so",
				Sink:   "fileOutput",
			},
		},
		Sources: map[string]input.SourceConfig{
			"fileInput": input.SourceConfig{
				Type: "File",
				FileConfig: input.FileConfig{
					Path: "testdata/pipelines/input",
				},
			},
		},
		Sinks: map[string]output.SinkConfig{
			"fileOutput": output.SinkConfig{
				Type: "File",
				FileConfig: output.FileConfig{
					Path: "testdata/pipelines/output",
				},
			},
		},
	}
	p, err := NewPipeline(pipelineConfig)
	if err != nil {
		t.Errorf("Error creating new pipeline: %s", err)
	}
	err = p.StartPipeline()
	if err != nil {
		t.Errorf("Error starting pipeline: %s", err)
	}
}
