package main

import (
	"errors"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/boltdb/bolt"
	"github.com/matryer/try"
)

type backend interface {
	Init() error
	Store(*pipeline) error
	Get(uuid []byte) ([]byte, error)
}

type backendConfig struct {
	Type           string         `json:"type"`
	BoltDBConfig   boltDBConfig   `json:"boldDBConfig,omitempty"`
	DynamoDBConfig dynamoDBConfig `json:"dynamoDBConfig,omitempty"`
}

func (bc backendConfig) Create() (backend, error) {
	switch bc.Type {
	case "boltdb":
		return &boltDBBackend{
			BucketName:   bc.BoltDBConfig.BucketName,
			DatabaseName: bc.BoltDBConfig.DatabaseName,
		}, nil
	case "dynamodb":
		session, err := session.NewSessionWithOptions(
			session.Options{
				SharedConfigState: session.SharedConfigEnable,
				Config:            aws.Config{Region: &bc.DynamoDBConfig.Region},
			},
		)
		if err != nil {
			return nil, err
		}

		if endpoint := os.Getenv("DYNAMODB_ENDPOINT"); endpoint != "" {
			session.Config.Endpoint = aws.String(endpoint)
		}
		return &dynamoDBBackend{
			svc:       dynamodb.New(session),
			TableName: bc.DynamoDBConfig.TableName,
		}, nil
	}
	return nil, errors.New("Invalid backend type " + bc.Type)
}

// BoltDB
type boltDBConfig struct {
	BucketName   string `json:"bucketName"`
	DatabaseName string `json:"databaseName"`
}

type boltDBBackend struct {
	db           *bolt.DB
	BucketName   string
	DatabaseName string
}

func (bb *boltDBBackend) Init() error {
	var err error
	bb.db, err = startBoltDB(bb.DatabaseName, bb.BucketName)
	return err
}

func (bb *boltDBBackend) Store(p *pipeline) error {
	key, err := (*p).ID.MarshalText()
	if err != nil {
		return err
	}

	value := (*p).Config
	return bb.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bb.BucketName))
		if b == nil {
			return errors.New("Bucket does not exist")
		}
		return b.Put(key, value)
	})
}

func (bb *boltDBBackend) Get(uuid []byte) ([]byte, error) {
	var value []byte
	err := bb.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bb.BucketName))
		value = b.Get(uuid)
		return nil
	})
	return value, err
}

// DynamoDB
type dynamoDBConfig struct {
	Region    string `json:"region"`
	TableName string `json:"tableName"`
}

type dynamoDBBackend struct {
	svc       dynamodbiface.DynamoDBAPI
	TableName string
	Retries   int
}

func (ddb *dynamoDBBackend) Init() error {
	session, err := session.NewSessionWithOptions(
		session.Options{
			SharedConfigState: session.SharedConfigEnable,
		},
	)
	if err != nil {
		return err
	}

	if endpoint := os.Getenv("DYNAMODB_ENDPOINT"); endpoint != "" {
		session.Config.Endpoint = aws.String(endpoint)
	}

	ddb.svc = dynamodb.New(session)
	return nil
}
func (ddb *dynamoDBBackend) Store(p *pipeline) error {
	key, err := (*p).ID.MarshalText()
	if err != nil {
		return err
	}

	value := (*p).Config
	dynamoValue := &dynamodb.PutItemInput{
		TableName: aws.String(ddb.TableName),
		Item: map[string]*dynamodb.AttributeValue{
			"UUID": {
				B: key,
			},
			"Config": {
				B: value,
			},
		},
	}
	return try.Do(func(attempt int) (bool, error) {
		_, err := ddb.svc.PutItem(dynamoValue)
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == dynamodb.ErrCodeProvisionedThroughputExceededException ||
				awsErr.Code() == dynamodb.ErrCodeInternalServerError &&
					attempt < ddb.Retries {
				// Backoff time as recommended by https://docs.aws.amazon.com/general/latest/gr/api-retries.html
				time.Sleep(time.Duration(2^attempt*100) * time.Millisecond)
				return true, err
			}
		}
		return false, err
	})
}

func (ddb *dynamoDBBackend) Get(uuid []byte) ([]byte, error) {
	var item *dynamodb.GetItemOutput
	err := try.Do(func(attempt int) (bool, error) {
		var err error
		item, err = ddb.svc.GetItem(&dynamodb.GetItemInput{
			TableName: aws.String(ddb.TableName),
			Key: map[string]*dynamodb.AttributeValue{
				"UUID": {
					B: uuid,
				},
			},
		})
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == dynamodb.ErrCodeProvisionedThroughputExceededException ||
				awsErr.Code() == dynamodb.ErrCodeInternalServerError &&
					attempt < ddb.Retries {
				// Backoff time as recommended by https://docs.aws.amazon.com/general/latest/gr/api-retries.html
				time.Sleep(time.Duration(2^attempt*100) * time.Millisecond)
				return true, err
			}
		}
		return false, err
	})
	return item.Item["Config"].B, err
}
