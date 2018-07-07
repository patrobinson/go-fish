package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/patrobinson/go-fish/input"
	"github.com/patrobinson/go-fish/output"
	"github.com/patrobinson/go-fish/state"
)

var basicRuleConfig = map[string]ruleConfig{
	"searchRule": ruleConfig{
		Source: "fileInput",
		State:  "searchConversion",
		Plugin: "testdata/rules/a.so",
		Sink:   "fileOutput",
	},
	"conversionRule": ruleConfig{
		Source: "fileInput",
		State:  "searchConversion",
		Plugin: "testdata/rules/length.so",
		Sink:   "fileOutput",
	},
}

var pipelineRuleConfig = map[string]ruleConfig{
	"searchRule": ruleConfig{
		Source: "fileInput",
		State:  "searchConversion",
		Plugin: "testdata/rules/a.so",
		Sink:   "conversionRule",
	},
	"conversionRule": ruleConfig{
		Source: "searchRule",
		State:  "searchConversion",
		Plugin: "testdata/rules/length.so",
		Sink:   "fileOutput",
	},
}

func makePipeline(rc map[string]ruleConfig) []byte {
	pipelineConfig, _ := json.Marshal(PipelineConfig{
		EventFolder: "testdata/eventTypes",
		Rules:       rc,
		States: map[string]state.StateConfig{
			"searchConversion": state.StateConfig{
				Type: "KV",
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
					Path: "testdata/output",
				},
			},
		},
	})
	return pipelineConfig
}

func TestParseConfig(t *testing.T) {
	testConfig, _ := os.Open("testdata/pipelines/config.json")
	testData, _ := ioutil.ReadAll(testConfig)
	var expectedConfig PipelineConfig
	json.Unmarshal(makePipeline(pipelineRuleConfig), &expectedConfig)

	parsedConfig, err := parseConfig(testData)
	if err != nil {
		t.Fatalf("Expected error to be nil, got %s", err)
	}
	if !reflect.DeepEqual(parsedConfig, expectedConfig) {
		t.Errorf("Expected config:\n%v\nGot config:\n%v", expectedConfig, parsedConfig)
	}
}

func TestNewPipeline(t *testing.T) {
	pipelineManager := &PipelineManager{
		backendConfig: backendConfig{
			Type: "boltdb",
			BoltDBConfig: boltDBConfig{
				BucketName:   "TestNewPipeline",
				DatabaseName: "test1.db",
			},
		},
	}
	err := pipelineManager.Init()
	if err != nil {
		t.Fatalf("Error creating Pipeline Manager: %s", err)
	}
	_, err = pipelineManager.NewPipeline(makePipeline(basicRuleConfig))
	if err != nil {
		t.Errorf("Error creating new pipeline: %s", err)
	}
}

func TestNewPipelineWithDuplicateKeys(t *testing.T) {
	pipelineConfig := PipelineConfig{
		EventFolder: "testdata/eventTypes",
		Rules: map[string]ruleConfig{
			"aRule": ruleConfig{
				Source: "aRule",
				Plugin: "testdata/rules/a.so",
			},
		},
		Sources: map[string]input.SourceConfig{
			"aRule": input.SourceConfig{
				Type: "File",
				FileConfig: input.FileConfig{
					Path: "testdata/pipelines/input",
				},
			},
		},
	}
	err := validateConfig(pipelineConfig)
	if err.Error() != "Invalid configuration, duplicate keys: [aRule]" {
		t.Errorf("Expected pipeline with duplicate keys to raise error, but got: %s", err)
	}
}

func TestStartBasicPipeline(t *testing.T) {
	pipelineManager := &PipelineManager{
		backendConfig: backendConfig{
			Type: "boltdb",
			BoltDBConfig: boltDBConfig{
				BucketName:   "TestStartBasicPipeline",
				DatabaseName: "test2.db",
			},
		},
	}
	err := pipelineManager.Init()
	if err != nil {
		t.Fatalf("Error creating Pipeline Manager: %s", err)
	}
	p, err := pipelineManager.NewPipeline(makePipeline(basicRuleConfig))
	if err != nil {
		t.Fatalf("Error creating new pipeline: %s", err)
	}
	err = p.StartPipeline()
	if err != nil {
		t.Errorf("Error starting pipeline: %s", err)
	}
}

func TestStartForwardPipeline(t *testing.T) {
	pipelineManager := &PipelineManager{
		backendConfig: backendConfig{
			Type: "boltdb",
			BoltDBConfig: boltDBConfig{
				BucketName:   "TestStartForwardPipeline",
				DatabaseName: "test3.db",
			},
		},
	}
	err := pipelineManager.Init()
	if err != nil {
		t.Fatalf("Error creating Pipeline Manager: %s", err)
	}
	p, err := pipelineManager.NewPipeline(makePipeline(pipelineRuleConfig))
	if err != nil {
		t.Fatalf("Error creating new pipeline: %s", err)
	}
	err = p.StartPipeline()
	if err != nil {
		t.Errorf("Error starting pipeline: %s", err)
	}
}
