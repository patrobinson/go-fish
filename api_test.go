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

	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

var pConfig = []byte(`
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

var a api

func TestMain(m *testing.M) {
	log.SetLevel(log.DebugLevel)
	a = api{}
	go a.Start(apiConfig{
		ListenAddress: ":8080",
		Backend: backendConfig{
			Type: "boltdb",
			BoltDBConfig: boltDBConfig{
				DatabaseName: "apitest.db",
				BucketName:   "apitest",
			},
		},
	})
	for {
		if a.apiReady {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	code := m.Run()
	os.Exit(code)
}

func executeRequest(req *http.Request) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	a.Router.ServeHTTP(rr, req)

	return rr
}

func TestGetPipelines(t *testing.T) {
	mConfig := monitoringConfiguration{
		MonitoringService: "", // Noop service
	}
	mService, err := mConfig.init(mux.NewRouter())
	pipeline, err := a.pipelineManager.NewPipeline(pConfig, mService)
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

	if !reflect.DeepEqual(response.Body.Bytes(), pConfig) {
		t.Errorf("Expected body to equal\n%s\nGot\n%s\n", pConfig, response.Body.String())
	}
	a.Shutdown()
}

func TestCreatePipeline(t *testing.T) {
	t.Logf("Posting to /pipelines")
	req, _ := http.NewRequest("POST", "/pipelines", bytes.NewReader(pConfig))

	response := executeRequest(req)
	if response.Code != 201 {
		t.Errorf("Expected 201 Created, got: %d", response.Code)
	}

	pID := response.Body.Bytes()
	config, _ := a.pipelineManager.Get(pID)
	if !reflect.DeepEqual(config, pConfig) {
		t.Errorf("Expected config\n%s\nGot\n%s", pConfig, config)
	}
	a.Shutdown()
}
