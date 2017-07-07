package kinesisStateStore

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

var tableName string

func CreateTable(tName string, svc dynamodbiface.DynamoDBAPI) error {
	tableName = tName
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("ShardID"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("ShardID"),
				KeyType:       aws.String("S"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
		TableName: aws.String(tableName),
	}
	_, err := svc.CreateTable(input)
	return err
}

func DoesTableExist(tableName string, svc dynamodbiface.DynamoDBAPI) bool {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}
	_, err := svc.DescribeTable(input)
	return (err == nil)
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
