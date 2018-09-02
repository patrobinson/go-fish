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
	debugFlag := flag.Bool("debug", false, "Set debug logging")
	checkRule := flag.String("checkRule", "", "Test a rule file to ensure it correctly implements the Rule interface")
	flag.Parse()

	if *debugFlag {
		log.SetLevel(log.DebugLevel)
	}

	switch {
	case *configFile != "":
		startFromConfig(*configFile)
	case *apiServer:
		startAPIFromConfig(*apiConfig)
	case *checkRule != "":
		testRule(*checkRule)
	default:
		log.Errorln("Please specify a valid command")
		flag.Usage()
	}
}

func startFromConfig(configFile string) {
	pManager := pipelineManager{
		backendConfig: backendConfig{
			Type: "boltdb",
			BoltDBConfig: boltDBConfig{
				BucketName:   "gofish",
				DatabaseName: "gofish.db",
			},
		},
	}
	err := pManager.Init()
	if err != nil {
		log.Fatal(err)
	}
	config, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Fatalf("Failed to open Config File: %v", err)
	}
	pipeline, err := pManager.NewPipeline(config)
	if err != nil {
		log.Fatal(err)
	}
	err = pipeline.StartPipeline()
	if err != nil {
		log.Fatal(err)
	}
}

func startAPIFromConfig(configFile string) {
	config, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Fatal(err)
	}
	apiConfig := parseAPIServerConfig(config)
	a := &api{}
	a.Start(apiConfig)
}
