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
	shard  shardStatus
	action string
}

var dynamosvc dynamodbiface.DynamoDBAPI

const tableName = "GoFish"

func setDynamoSvc(svc dynamodbiface.DynamoDBAPI) {
	dynamosvc = svc
}

// Init implements intialises the Input mechanism
func (ki *KinesisInput) Init() error {
	session, err := session.NewSessionWithOptions(session.Options{SharedConfigState: session.SharedConfigEnable})
	if err != nil {
		return err
	}
	ki.kinesisSvc = kinesis.New(session)
	setDynamoSvc(dynamodb.New(session))
	ki.setupDynamo()
	err = ki.setupKinesis()
	return err
}

func (ki *KinesisInput) setupDynamo() {
	if !kinesisStateStore.DoesTableExist(tableName, dynamosvc) {
		kinesisStateStore.CreateTable(tableName, dynamosvc)
	}
}

func (ki *KinesisInput) setupKinesis() error {
	shardIds, err := getShardIds(ki.kinesisSvc, dynamosvc, ki.StreamName, "")
	if err != nil {
		return err
	}
	ki.shardIds = shardIds

	shardChan := make(chan shardChange)
	ki.shardMgmt = &shardChan
	go ki.shardIDManager()
	return nil
}

func getShardIds(kinesisSvc kinesisiface.KinesisAPI, dynamoSvc dynamodbiface.DynamoDBAPI, streamName string, startShardID string) (map[string]shardStatus, error) {
	shards := map[string]shardStatus{}
	args := &kinesis.DescribeStreamInput{
		StreamName: aws.String(streamName),
	}
	streamDesc, err := kinesisSvc.DescribeStream(args)
	if err != nil {
		return shards, err
	}

	if *streamDesc.StreamDescription.StreamStatus != "ACTIVE" {
		return shards, errors.New("Stream not active")
	}

	var lastShardID string
	for _, s := range streamDesc.StreamDescription.Shards {
		/* Retrieve shard checkpoint from DynamoDB */
		checkpoint, err := kinesisStateStore.GetItem(*s.ShardId, dynamoSvc)
		if err != nil {
			return shards, err
		}
		if _, ok := checkpoint["ShardID"]; ok {
			shards[*s.ShardId], err = unmarshalShardStatus(checkpoint)
			if err != nil {
				log.Errorf("Unable to unmarshal shard checkpoint %v", err)
				return shards, err
			}
		} else {
			shards[*s.ShardId] = shardStatus{
				ShardID: *s.ShardId,
			}
		}
		lastShardID = *s.ShardId
	}

	if *streamDesc.StreamDescription.HasMoreShards {
		moreShards, err := getShardIds(kinesisSvc, dynamoSvc, streamName, lastShardID)
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
			switch shardChange.action {
			case "DELETE":
				delete(ki.shardIds, shardChange.shard.ShardID)
			case "SAVE":
				if err := shardChange.shard.Save(); err != nil {
					log.Errorf("Error saving Shard Status: %v", err)
				}
			default:
				log.Errorf("Invalid action received to shardID Manager: %v", shardChange.action)
			}
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
			sChange := shardChange{
				shard: shardStatus{
					ShardID: shardID,
				},
				action: "DELETE",
			}
			*ki.shardMgmt <- sChange
		} else {
			shard.Checkpoint = *getResp.NextShardIterator
			*ki.shardMgmt <- shardChange{
				shard: shardStatus{
					ShardID: shardID,
				},
				action: "SAVE",
			}
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
