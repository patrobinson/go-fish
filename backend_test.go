package main

import (
	"errors"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/google/uuid"
)

type mockDynamoDB struct {
	dynamodbiface.DynamoDBAPI
	tableExist bool
	item       map[string]*dynamodb.AttributeValue
}

func (m *mockDynamoDB) DescribeTable(*dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
	if !m.tableExist {
		return &dynamodb.DescribeTableOutput{}, awserr.New(dynamodb.ErrCodeResourceNotFoundException, "doesNotExist", errors.New(""))
	}
	return &dynamodb.DescribeTableOutput{}, nil
}

func (m *mockDynamoDB) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	m.item = input.Item
	return nil, nil
}

func (m *mockDynamoDB) GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	return &dynamodb.GetItemOutput{
		Item: m.item,
	}, nil
}

func TestDynamoGetStoredPipeline(t *testing.T) {
	pipelineConfig := []byte(`
		{
			"eventFolder": "testdata/eventTypes",
			"sources": {
				"fileInput": {
				  "type": "File",
				  "file_config": {
					"path": "testdata/pipelines/input/file.in"
				  }
				}
			},
			"rules": {
				"searchRule": {
				  "source": "fileInput",
				  "plugin": "testdata/rules/a.so"
				}
			},
			"states": {},
			"sinks": {}
		}
	`)
	id := uuid.New()
	idVal, _ := id.MarshalText()
	pipeline := Pipeline{
		ID:     id,
		Config: pipelineConfig,
	}

	backend := &dynamoDBBackend{
		svc: &mockDynamoDB{
			tableExist: true,
		},
		TableName: "go-fish",
		Retries:   0,
	}
	err := backend.Store(&pipeline)
	if err != nil {
		t.Fatalf("Error storing pipeline %s", err)
	}
	storedBackend, err := backend.Get(idVal)
	if err != nil {
		t.Fatalf("Error retrieving pipeline %s", err)
	}
	if !reflect.DeepEqual(storedBackend, pipelineConfig) {
		t.Errorf("Expected config %s\nGot %s", pipelineConfig, storedBackend)
	}
}
