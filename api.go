package main

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

type API struct {
	pipelineManager *PipelineManager
	Router          *mux.Router
	httpServer      *http.Server
}

// Start starts the API server and blocks
func (api *API) Start(config apiConfig) {
	api.pipelineManager = &PipelineManager{
		backendConfig: config.Backend,
	}
	err := api.pipelineManager.Init()
	if err != nil {
		log.Fatal(err)
	}

	api.Router = mux.NewRouter()
	api.httpServer = &http.Server{
		Addr:         config.ListenAddress,
		WriteTimeout: time.Second * 15,
		ReadTimeout:  time.Second * 15,
		IdleTimeout:  time.Second * 60,
		Handler:      api.Router,
	}

	api.Router.Path("/pipelines/{id}").Methods("GET").HandlerFunc(api.GetPipelines)
	api.Router.Path("/pipelines").Methods("POST").HandlerFunc(api.CreatePipeline)
	err = api.httpServer.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

// Shutdown the API Server
func (api *API) Shutdown() {
	log.Info("Shutting down API Server")
	api.httpServer.Shutdown(context.Background())
}

// GetPipelines gets a list of Pipelines
func (api *API) GetPipelines(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	pipelineID := vars["id"]
	// Validation of incoming variable?
	pipeline, err := api.pipelineManager.Get([]byte(pipelineID))
	if err != nil {
		w.WriteHeader(500)
		// Should probably wrap that err
		w.Write([]byte(err.Error()))
	}
	log.Debugf("Response from pipeline manager for %s: %s", pipelineID, pipeline)
	if len(pipeline) == 0 {
		w.WriteHeader(404)
		return
	}

	w.Write(pipeline)
}

// CreatePipeline creates a new Pipeline and returns the UUID
func (api *API) CreatePipeline(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Errorln("Error reading body", err)
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
		return
	}
	if len(body) == 0 {
		log.Errorln("Empty body received")
		w.WriteHeader(400)
		w.Write([]byte("No pipeline config received"))
		return
	}
	log.Debugln("Creating pipeline with config", string(body))
	pipeline, err := api.pipelineManager.NewPipeline(body)
	if err != nil {
		log.Errorln("Error creating pipeline", err)
		w.WriteHeader(400)
		w.Write([]byte(err.Error()))
		return
	}
	log.Debugln("Created pipeline", pipeline.ID)
	err = api.pipelineManager.Store(pipeline)
	if err != nil {
		log.Errorln("Error storing pipeline", err)
		w.WriteHeader(500)
		w.Write([]byte(err.Error()))
		return
	}
	go func() {
		err := pipeline.StartPipeline()
		if err != nil {
			log.Errorln("Pipeline failed:", err)
		}
	}()
	w.WriteHeader(201)
	w.Write([]byte(pipeline.ID.String()))
}

type apiConfig struct {
	ListenAddress string        `json:"listenAddress"`
	Backend       backendConfig `json:"backendConfig"`
}

func parseAPIServerConfig(config []byte) apiConfig {
	var c apiConfig
	json.Unmarshal(config, &c)
	if c.ListenAddress == "" {
		c.ListenAddress = ":8000"
	}
	return c
}
