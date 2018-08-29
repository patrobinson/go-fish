package main

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
)

var pipelineConfig = []byte(`
	{
		"eventFolder": "testdata/eventTypes",
		"sources": {
			"fileInput": {
			  "type": "File",
			  "file_config": {
				"path": "testdata/pipelines/input"
			  }
			}
		},
		"rules": {
			"searchRule": {
			  "source": "fileInput",
			  "plugin": "testdata/rules/a.so",
			  "sink": "fileOutput"
			}
		},
		"states": {},
		"sinks": {
			"fileOutput": {
				"type": "File",
				"file_config": {
				  "path": "testdata/pipelines/output"
				}
			  }
		}
	}
`)

var api API

func TestMain(m *testing.M) {
	log.SetLevel(log.DebugLevel)
	api = API{}
	go api.Start(apiConfig{
		ListenAddress: ":8080",
		Backend: backendConfig{
			Type: "boltdb",
			BoltDBConfig: boltDBConfig{
				DatabaseName: "apitest.db",
				BucketName:   "apitest",
			},
		},
	})
	time.Sleep(200 * time.Millisecond)
	code := m.Run()
	os.Exit(code)
}

func executeRequest(req *http.Request) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	api.Router.ServeHTTP(rr, req)

	return rr
}

func TestGetPipelines(t *testing.T) {

	pipeline, err := api.pipelineManager.NewPipeline(pipelineConfig)
	if err != nil {
		t.Fatalf("Error creating pipeline %s", err)
	}

	pID, _ := (*pipeline).ID.MarshalText()
	t.Logf(fmt.Sprintf("Getting /pipelines/%s", pID))
	req, _ := http.NewRequest("GET", fmt.Sprintf("/pipelines/%s", pID), nil)
	response := executeRequest(req)
	if response.Code != 200 {
		t.Errorf("Expected 200 OK, got: %d", response.Code)
	}

	if !reflect.DeepEqual(response.Body.Bytes(), pipelineConfig) {
		t.Errorf("Expected body to equal\n%s\nGot\n%s\n", pipelineConfig, response.Body.String())
	}
	api.Shutdown()
}

func TestCreatePipeline(t *testing.T) {
	t.Logf("Posting to /pipelines")
	req, _ := http.NewRequest("POST", "/pipelines", bytes.NewReader(pipelineConfig))

	response := executeRequest(req)
	if response.Code != 201 {
		t.Errorf("Expected 201 Created, got: %d", response.Code)
	}

	pID := response.Body.Bytes()
	config, _ := api.pipelineManager.Get(pID)
	if !reflect.DeepEqual(config, pipelineConfig) {
		t.Errorf("Expected config\n%s\nGot\n%s", pipelineConfig, config)
	}
	api.Shutdown()
}
