package gokini

import (
	"errors"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/matryer/try"
	log "github.com/sirupsen/logrus"
)

const (
	defaultLeaseDuration = 30000
	// ErrLeaseNotAquired is returned when we failed to get a lock on the shard
	ErrLeaseNotAquired = "Lease is already held by another node"
	// ErrInvalidDynamoDBSchema is returned when there are one or more fields missing from the table
	ErrInvalidDynamoDBSchema = "The DynamoDB schema is invalid and may need to be re-created"
)

// Checkpointer handles checkpointing when a record has been processed
type Checkpointer interface {
	Init() error
	StealLease(*shardStatus, string) error
	GetLease(*shardStatus, string) error
	CheckpointSequence(*shardStatus) error
	FetchCheckpoint(*shardStatus) error
}

// ErrSequenceIDNotFound is returned by FetchCheckpoint when no SequenceID is found
var ErrSequenceIDNotFound = errors.New("SequenceIDNotFoundForShard")

// DynamoCheckpoint implements the Checkpoint interface using DynamoDB as a backend
type DynamoCheckpoint struct {
	TableName     string
	LeaseDuration int
	svc           dynamodbiface.DynamoDBAPI
	Retries       int
}

// Init initialises the DynamoDB Checkpoint
func (checkpointer *DynamoCheckpoint) Init() error {
	log.Debug("Creating DynamoDB session")
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

	checkpointer.svc = dynamodb.New(session)

	if checkpointer.LeaseDuration == 0 {
		checkpointer.LeaseDuration = defaultLeaseDuration
	}

	if !checkpointer.doesTableExist() {
		return checkpointer.createTable()
	}
	return nil
}

// StealLease attempts to take a lease away from another node.
func (checkpointer *DynamoCheckpoint) StealLease(shard *shardStatus, newAssignTo string) error {
	return checkpointer.aquireLease(shard, newAssignTo, true)
}

// GetLease attempts to gain a lock on the given shard
func (checkpointer *DynamoCheckpoint) GetLease(shard *shardStatus, newAssignTo string) error {
	return checkpointer.aquireLease(shard, newAssignTo, false)
}
func (checkpointer *DynamoCheckpoint) aquireLease(shard *shardStatus, newAssignTo string, force bool) error {
	newLeaseTimeout := time.Now().Add(time.Duration(checkpointer.LeaseDuration) * time.Millisecond).UTC()
	newLeaseTimeoutString := newLeaseTimeout.Format(time.RFC3339)
	currentCheckpoint, err := checkpointer.getItem(shard.ID)
	if err != nil {
		return err
	}

	assignedVar, assignedToOk := currentCheckpoint["AssignedTo"]
	leaseVar, leaseTimeoutOk := currentCheckpoint["LeaseTimeout"]
	var conditionalExpression string
	var expressionAttributeValues map[string]*dynamodb.AttributeValue
	if !leaseTimeoutOk || !assignedToOk {
		conditionalExpression = "attribute_not_exists(AssignedTo)"
	} else {
		assignedTo := *assignedVar.S
		leaseTimeout := *leaseVar.S

		currentLeaseTimeout, err := time.Parse(time.RFC3339, leaseTimeout)
		if err != nil {
			return err
		}
		if (!time.Now().UTC().After(currentLeaseTimeout) && assignedTo != newAssignTo) && !force {
			return errors.New(ErrLeaseNotAquired)
		}
		log.Debugf("Attempting to get a lock for shard: %s, leaseTimeout: %s, assignedTo: %s", shard.ID, currentLeaseTimeout, assignedTo)
		conditionalExpression = "ShardID = :id AND AssignedTo = :assigned_to AND LeaseTimeout = :lease_timeout"
		expressionAttributeValues = map[string]*dynamodb.AttributeValue{
			":id": {
				S: &shard.ID,
			},
			":assigned_to": {
				S: &assignedTo,
			},
			":lease_timeout": {
				S: &leaseTimeout,
			},
		}
	}

	marshalledCheckpoint := map[string]*dynamodb.AttributeValue{
		"ShardID": {
			S: &shard.ID,
		},
		"AssignedTo": {
			S: &newAssignTo,
		},
		"LeaseTimeout": {
			S: &newLeaseTimeoutString,
		},
	}

	if shard.Checkpoint != "" {
		marshalledCheckpoint["Checkpoint"] = &dynamodb.AttributeValue{
			S: &shard.Checkpoint,
		}
	}

	err = checkpointer.conditionalUpdate(conditionalExpression, expressionAttributeValues, marshalledCheckpoint)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == dynamodb.ErrCodeConditionalCheckFailedException {
				return errors.New(ErrLeaseNotAquired)
			}
		}
		return err
	}

	shard.mux.Lock()
	shard.AssignedTo = newAssignTo
	shard.LeaseTimeout = newLeaseTimeout
	shard.mux.Unlock()

	return nil
}

// CheckpointSequence writes a checkpoint at the designated sequence ID
func (checkpointer *DynamoCheckpoint) CheckpointSequence(shard *shardStatus) error {
	leaseTimeout := shard.LeaseTimeout.UTC().Format(time.RFC3339)
	marshalledCheckpoint := map[string]*dynamodb.AttributeValue{
		"ShardID": {
			S: &shard.ID,
		},
		"SequenceID": {
			S: &shard.Checkpoint,
		},
		"AssignedTo": {
			S: &shard.AssignedTo,
		},
		"LeaseTimeout": {
			S: &leaseTimeout,
		},
	}
	return checkpointer.saveItem(marshalledCheckpoint)
}

// FetchCheckpoint retrieves the checkpoint for the given shard
func (checkpointer *DynamoCheckpoint) FetchCheckpoint(shard *shardStatus) error {
	checkpoint, err := checkpointer.getItem(shard.ID)
	if err != nil {
		return err
	}

	sequenceID, ok := checkpoint["SequenceID"]
	if !ok {
		return ErrSequenceIDNotFound
	}
	log.Debugf("Retrieved Shard Iterator %s", *sequenceID.S)
	shard.mux.Lock()
	defer shard.mux.Unlock()
	shard.Checkpoint = *sequenceID.S

	if assignedTo, ok := checkpoint["AssignedTo"]; ok {
		shard.AssignedTo = *assignedTo.S
	}
	return nil
}

func (checkpointer *DynamoCheckpoint) createTable() error {
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
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
		TableName: aws.String(checkpointer.TableName),
	}
	_, err := checkpointer.svc.CreateTable(input)
	return err
}

func (checkpointer *DynamoCheckpoint) doesTableExist() bool {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(checkpointer.TableName),
	}
	_, err := checkpointer.svc.DescribeTable(input)
	return (err == nil)
}

func (checkpointer *DynamoCheckpoint) saveItem(item map[string]*dynamodb.AttributeValue) error {
	return checkpointer.putItem(&dynamodb.PutItemInput{
		TableName: aws.String(checkpointer.TableName),
		Item:      item,
	})
}

func (checkpointer *DynamoCheckpoint) conditionalUpdate(conditionExpression string, expressionAttributeValues map[string]*dynamodb.AttributeValue, item map[string]*dynamodb.AttributeValue) error {
	return checkpointer.putItem(&dynamodb.PutItemInput{
		ConditionExpression: aws.String(conditionExpression),
		TableName:           aws.String(checkpointer.TableName),
		Item:                item,
		ExpressionAttributeValues: expressionAttributeValues,
	})
}

func (checkpointer *DynamoCheckpoint) putItem(input *dynamodb.PutItemInput) error {
	return try.Do(func(attempt int) (bool, error) {
		_, err := checkpointer.svc.PutItem(input)
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == dynamodb.ErrCodeProvisionedThroughputExceededException ||
				awsErr.Code() == dynamodb.ErrCodeInternalServerError &&
					attempt < checkpointer.Retries {
				// Backoff time as recommended by https://docs.aws.amazon.com/general/latest/gr/api-retries.html
				time.Sleep(time.Duration(2^attempt*100) * time.Millisecond)
				return true, err
			}
		}
		return false, err
	})
}

func (checkpointer *DynamoCheckpoint) getItem(shardID string) (map[string]*dynamodb.AttributeValue, error) {
	var item *dynamodb.GetItemOutput
	err := try.Do(func(attempt int) (bool, error) {
		var err error
		item, err = checkpointer.svc.GetItem(&dynamodb.GetItemInput{
			TableName: aws.String(checkpointer.TableName),
			Key: map[string]*dynamodb.AttributeValue{
				"ShardID": {
					S: aws.String(shardID),
				},
			},
		})
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == dynamodb.ErrCodeProvisionedThroughputExceededException ||
				awsErr.Code() == dynamodb.ErrCodeInternalServerError &&
					attempt < checkpointer.Retries {
				// Backoff time as recommended by https://docs.aws.amazon.com/general/latest/gr/api-retries.html
				time.Sleep(time.Duration(2^attempt*100) * time.Millisecond)
				return true, err
			}
		}
		return false, err
	})
	return item.Item, err
}
