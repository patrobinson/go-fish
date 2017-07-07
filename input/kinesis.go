package input

import (
	"errors"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/patrobinson/go-fish/input/kinesisStateStore"
)

// KinesisInput implements the Input interface
type KinesisInput struct {
	outputChan *chan []byte
	shardIds   map[string]shardStatus
	shardMgmt  *chan shardChange
	StreamName string
	kinesisSvc kinesisiface.KinesisAPI
}

type shardStatus struct {
	ShardID    string
	Checkpoint string
}

type shardChange struct {
	shardID string
	action  string
}

var dynamosvc dynamodbiface.DynamoDBAPI

const tableName = "GoFish"

// Init implements intialises the Input mechanism
func (ki *KinesisInput) Init() error {
	session, err := session.NewSessionWithOptions(session.Options{SharedConfigState: session.SharedConfigEnable})
	if err != nil {
		return err
	}
	ki.kinesisSvc = kinesis.New(session)
	shardIds, err := getShardIds(ki.kinesisSvc, ki.StreamName, "")
	ki.shardIds = shardIds

	dynamosvc = dynamodb.New(session)
	if !kinesisStateStore.DoesTableExist(tableName, dynamosvc) {
		kinesisStateStore.CreateTable(tableName, dynamosvc)
	}

	shardChan := make(chan shardChange)
	ki.shardMgmt = &shardChan
	go ki.shardIDManager()
	return err
}

func getShardIds(svc kinesisiface.KinesisAPI, streamName string, startShardID string) (map[string]shardStatus, error) {
	shards := map[string]shardStatus{}
	args := &kinesis.DescribeStreamInput{
		StreamName: aws.String(streamName),
	}
	streamDesc, err := svc.DescribeStream(args)
	if err != nil {
		return shards, err
	}

	if *streamDesc.StreamDescription.StreamStatus != "ACTIVE" {
		return shards, errors.New("Stream not active")
	}

	var lastShardID string
	for _, s := range streamDesc.StreamDescription.Shards {
		shards[*s.ShardId] = shardStatus{
			ShardID: *s.ShardId,
		}
		lastShardID = *s.ShardId
	}

	if *streamDesc.StreamDescription.HasMoreShards {
		moreShards, err := getShardIds(svc, streamName, lastShardID)
		if err != nil {
			return shards, err
		}
		for k, v := range moreShards {
			shards[k] = v
		}
	}

	return shards, nil
}

func (ki *KinesisInput) shardIDManager() {
	for {
		select {
		case shardChange := <-*ki.shardMgmt:
			if shardChange.action != "DELETE" {
				log.Errorf("Invalid action received to shardID Manager: %v", shardChange.action)
			}
			delete(ki.shardIds, shardChange.shardID)
		default:
			time.Sleep(1000 * time.Millisecond)
		}
	}
}

func (ki *KinesisInput) getRecords(shardID string, output *chan []byte) {
	svc := ki.kinesisSvc
	shardIterArgs := &kinesis.GetShardIteratorInput{
		ShardId:           aws.String(shardID),
		ShardIteratorType: aws.String("TRIM_HORIZON"),
		StreamName:        aws.String(ki.StreamName),
	}
	iterResp, err := svc.GetShardIterator(shardIterArgs)
	if err != nil {
		log.Fatalf("Unable to retrieve records: %v", err)
	}
	shard := ki.shardIds[shardID]
	shard.Checkpoint = *iterResp.ShardIterator

	for {
		getRecordsArgs := &kinesis.GetRecordsInput{
			ShardIterator: aws.String(shard.Checkpoint),
		}
		getResp, err := svc.GetRecords(getRecordsArgs)
		if err != nil {
			log.Errorf("Error getting records from shard %v: %v", shardID, err)
			continue
		}
		if getResp.NextShardIterator == nil {
			*ki.shardMgmt <- shardChange{
				shardID: shardID,
				action:  "DELETE",
			}
		} else {
			shard.Checkpoint = *getResp.NextShardIterator
		}

		for i := range getResp.Records {
			*ki.outputChan <- getResp.Records[i].Data
		}
	}
}

// Retrieve implements the Input interface
func (ki *KinesisInput) Retrieve(output *chan []byte) {
	ki.outputChan = output
	for _, s := range ki.shardIds {
		go ki.getRecords(s.ShardID, output)
	}
}

func (ss *shardStatus) Marshal() (map[string]*dynamodb.AttributeValue, error) {
	ms, err := dynamodbattribute.MarshalMap(ss)
	return ms, err
}

func (ss *shardStatus) Save() error {
	ms, err := ss.Marshal()
	if err != nil {
		return err
	}
	return kinesisStateStore.SaveItem(ms, dynamosvc)
}

func unmarshalShardStatus(ms map[string]*dynamodb.AttributeValue) (shardStatus, error) {
	var ss shardStatus
	err := dynamodbattribute.UnmarshalMap(ms, &ss)
	return ss, err
}
