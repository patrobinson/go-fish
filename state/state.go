package state

import (
	"fmt"
)

// Config defines the configuration for a state
type Config struct {
	Type     string   `json:"type"`
	KVConfig KVConfig `json:"kvConfig,omitempty"`
}

// State is the interface for stateful storage backings for a rule
type State interface {
	Init() error
	Close()
}

// Create creates the stateful backing
func Create(config Config) (State, error) {
	switch config.Type {
	case "KV":
		return &KVStore{
			DbFileName: config.KVConfig.DbFileName,
			BucketName: config.KVConfig.BucketName,
		}, nil
	case "Count":
		return &Counter{}, nil
	}

	return nil, fmt.Errorf("Invalid state type: %v", config.Type)
}
