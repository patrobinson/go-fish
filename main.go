package main

import (
	"flag"
	"io/ioutil"

	log "github.com/sirupsen/logrus"
)

func main() {
	apiServer := flag.Bool("apiServer", false, "Start an API Server")
	apiConfig := flag.String("apiConfig", "", "API Server configuration")
	configFile := flag.String("pipelineConfig", "", "Pipeline Config")
	flag.Parse()

	if (*configFile == "" && !*apiServer) || (*apiServer && *configFile != "") {
		log.Fatal("You must either specify a configuration file or start an API Server")
	}

	if *configFile != "" {
		startFromConfig(*configFile)
	}

	if *apiServer {
		startAPIFromConfig(*apiConfig)
	}
}

func startFromConfig(configFile string) {
	pipelineManager := PipelineManager{
		backendConfig: backendConfig{
			Type: "boltdb",
			BoltDBConfig: boltDBConfig{
				BucketName:   "gofish",
				DatabaseName: "gofish.db",
			},
		},
	}
	err := pipelineManager.Init()
	if err != nil {
		log.Fatal(err)
	}
	config, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Fatalf("Failed to open Config File: %v", err)
	}
	pipeline, err := pipelineManager.NewPipeline(config)
	if err != nil {
		log.Fatal(err)
	}
	err = pipeline.StartPipeline()
	if err != nil {
		log.Fatal(err)
	}

	pipeline.Run()
}

func startAPIFromConfig(configFile string) {
	config, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Fatal(err)
	}
	apiConfig := parseAPIServerConfig(config)
	api := &API{}
	api.Start(apiConfig)
}
