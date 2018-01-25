package main

import (
	"flag"
	"os"

	log "github.com/Sirupsen/logrus"
)

func main() {
	configFile := flag.String("config", "", "Pipeline Config")
	flag.Parse()
	file, err := os.Open(*configFile)
	if err != nil {
		log.Fatalf("Failed to open Config File: %v", err)
	}
	config, err := parseConfig(file)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	err = validateConfig(config)
	if err != nil {
		log.Fatal(err)
	}

	pipeline, err := NewPipeline(config)
	if err != nil {
		log.Fatal(err)
	}

	err = pipeline.StartPipeline()
	if err != nil {
		log.Fatal(err)
	}

	pipeline.Run()
}
