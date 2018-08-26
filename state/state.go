package state

import (
	"fmt"
)

type StateConfig struct {
	Type     string   `json:"type"`
	KVConfig KVConfig `json:"kvConfig,omitempty"`
}

// Sink is an interface for output implementations
type State interface {
	Init() error
	Close()
}

func Create(config StateConfig) (State, error) {
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
