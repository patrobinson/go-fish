package kinesisStateStore

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

const tableName = "GoFish"

func createTable(tableName string) error {
	return nil
}

func doesTableExist(tableName string) bool {
	return false
}

func SaveItem(item *dynamodb.AttributeValue, svc dynamodbiface.DynamoDBAPI) error {
	return nil
}
