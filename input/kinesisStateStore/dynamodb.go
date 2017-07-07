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

// SaveItem saves a dynamo attribute value to the predefined table
func SaveItem(item map[string]*dynamodb.AttributeValue, svc dynamodbiface.DynamoDBAPI) error {
	_, err := svc.PutItem(&dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      item,
	})
	return err
}

// GetItem returns a dynamo row givenn a ShardID
func GetItem(shardID string, svc dynamodbiface.DynamoDBAPI) (map[string]*dynamodb.AttributeValue, error) {
	item, err := svc.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]*dynamodb.AttributeValue{
			"ShardID": {
				S: aws.String(shardID),
			},
		},
	})
	return item.Item, err

}
