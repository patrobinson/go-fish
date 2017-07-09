package input

import (
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/patrobinson/go-fish/input/kinesisStateStore"
)

type mockDynamoClient struct {
	dynamodbiface.DynamoDBAPI
}

func (m *mockDynamoClient) DescribeTable(*dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
	return &dynamodb.DescribeTableOutput{}, nil
}

func (m *mockDynamoClient) PutItem(*dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	return &dynamodb.PutItemOutput{}, nil
}

func (m *mockDynamoClient) GetItem(*dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	return &dynamodb.GetItemOutput{}, nil
}

func setupInput(mockClient kinesisiface.KinesisAPI) *KinesisInput {
	outChan := make(chan []byte)
	mgmtChan := make(chan shardChange)
	kInput := &KinesisInput{
		outputChan: &outChan,
		shardMgmt:  &mgmtChan,
		StreamName: "testStream",
		kinesisSvc: mockClient,
		shardIds: map[string]shardStatus{
			"00000001": shardStatus{
				ShardID: "00000001",
			},
		},
	}
	setDynamoSvc(&mockDynamoClient{})
	kInput.setupDynamo()
	kInput.setupKinesis()
	return kInput
}

type mockKClientGet struct {
	kinesisiface.KinesisAPI
}

func (k *mockKClientGet) GetShardIterator(args *kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
	return &kinesis.GetShardIteratorOutput{
		ShardIterator: aws.String("0123456789ABCDEF"),
	}, nil
}

func (k *mockKClientGet) DescribeStream(args *kinesis.DescribeStreamInput) (*kinesis.DescribeStreamOutput, error) {
	return &kinesis.DescribeStreamOutput{
		StreamDescription: &kinesis.StreamDescription{
			StreamStatus: aws.String("ACTIVE"),
			Shards: []*kinesis.Shard{
				&kinesis.Shard{
					ShardId: aws.String("00000001"),
				},
			},
			HasMoreShards: aws.Bool(false),
		},
	}, nil
}

func (k *mockKClientGet) GetRecords(args *kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {
	return &kinesis.GetRecordsOutput{
		MillisBehindLatest: aws.Int64(0),
		NextShardIterator:  aws.String("ABCD1234"),
		Records: []*kinesis.Record{
			&kinesis.Record{
				Data: []byte("Hello World"),
			},
		},
	}, nil
}
func TestSendRecordsToChannel(t *testing.T) {
	input := setupInput(&mockKClientGet{})
	inputC := make(chan []byte)
	go input.Retrieve(&inputC)
	msg := <-inputC
	if string(msg) != "Hello World" {
		t.Errorf("Message received does not match %v", msg)
	}
}

type mockKClientClosedShard struct {
	kinesisiface.KinesisAPI
}

func (k *mockKClientClosedShard) GetShardIterator(args *kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
	return &kinesis.GetShardIteratorOutput{
		ShardIterator: aws.String("0123456789ABCDEF"),
	}, nil
}

func (k *mockKClientClosedShard) GetRecords(args *kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {
	return &kinesis.GetRecordsOutput{
		MillisBehindLatest: aws.Int64(0),
		NextShardIterator:  nil,
		Records: []*kinesis.Record{
			&kinesis.Record{
				Data: []byte("Hello World"),
			},
		},
	}, nil
}

func (k *mockKClientClosedShard) DescribeStream(args *kinesis.DescribeStreamInput) (*kinesis.DescribeStreamOutput, error) {
	return &kinesis.DescribeStreamOutput{
		StreamDescription: &kinesis.StreamDescription{
			StreamStatus: aws.String("ACTIVE"),
			Shards: []*kinesis.Shard{
				&kinesis.Shard{
					ShardId: aws.String("00000001"),
				},
			},
			HasMoreShards: aws.Bool(false),
		},
	}, nil
}

func TestShardClosed(t *testing.T) {
	input := setupInput(&mockKClientClosedShard{})
	inputC := make(chan []byte)
	go input.Retrieve(&inputC)
	m := <-inputC
	if _, ok := input.shardIds["00000001"]; ok {
		t.Errorf("Shard not deleted %v", m)
	}
}

type mockKClientGetShards struct {
	kinesisiface.KinesisAPI
}

func (k *mockKClientGetShards) DescribeStream(args *kinesis.DescribeStreamInput) (*kinesis.DescribeStreamOutput, error) {
	return &kinesis.DescribeStreamOutput{
		StreamDescription: &kinesis.StreamDescription{
			StreamStatus:  aws.String("ACTIVE"),
			HasMoreShards: aws.Bool(false),
			Shards: []*kinesis.Shard{
				&kinesis.Shard{
					ShardId: aws.String("00000001"),
				},
			},
		},
	}, nil
}

type mockDynamoClienGet struct {
	dynamodbiface.DynamoDBAPI
	items map[string]string
}

func (m *mockDynamoClienGet) GetItem(params *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	shardID := params.Key["ShardID"].S
	checkpoint := m.items[*shardID]
	result := &dynamodb.GetItemOutput{
		Item: map[string]*dynamodb.AttributeValue{
			"ShardID":    {S: shardID},
			"Checkpoint": {S: &checkpoint},
		},
	}
	return result, nil
}

func TestGetShardIds(t *testing.T) {
	kinesisClient := &mockKClientGetShards{}
	dynamodbClient := &mockDynamoClienGet{
		items: map[string]string{},
	}

	shards, err := getShardIds(kinesisClient, dynamodbClient, "testStream", "")
	if err != nil {
		t.Errorf("Error getting Shards: %v", err)
	}
	if v, ok := shards["00000001"]; !ok || v.ShardID != "00000001" {
		t.Errorf("Invalid Shard ID returned")
	}
	if len(shards) != 1 {
		t.Errorf("Too many shards")
	}

	dynamodbClient = &mockDynamoClienGet{
		items: map[string]string{
			"00000001": "0123456789ABCDEF",
		},
	}

	shards, err = getShardIds(kinesisClient, dynamodbClient, "testStream", "")
	if err != nil {
		t.Errorf("Error getting Shards: %v", err)
	}
	if v, ok := shards["00000001"]; !ok || v.Checkpoint != "0123456789ABCDEF" {
		t.Errorf("Checkpoint not restored from saved point")
	}
}

func TestMarshalShardStatus(t *testing.T) {
	shardS := &shardStatus{
		ShardID:    "00000001",
		Checkpoint: "0123456789ABCDEF",
	}
	shardSItem, err := shardS.Marshal()
	if err != nil {
		t.Errorf("Unable to Marshal shard status: %v", err)
	}
	expect := map[string]*dynamodb.AttributeValue{
		"ShardID":    {S: aws.String("00000001")},
		"Checkpoint": {S: aws.String("0123456789ABCDEF")},
	}
	if !reflect.DeepEqual(expect, shardSItem) {
		t.Errorf("Expected %v, got %v", expect, shardSItem)
	}
}

func TestUnmarshalShardStatus(t *testing.T) {
	ms := map[string]*dynamodb.AttributeValue{
		"ShardID":    {S: aws.String("00000001")},
		"Checkpoint": {S: aws.String("0123456789ABCDEF")},
	}

	expect := shardStatus{
		ShardID:    "00000001",
		Checkpoint: "0123456789ABCDEF",
	}

	ss, err := unmarshalShardStatus(ms)
	if err != nil {
		t.Errorf("Uname to Unmarshal shard status: %v", err)
	}

	if !reflect.DeepEqual(expect, ss) {
		t.Errorf("Expected %v, got %v", expect, ss)
	}
}

type mockDynamoClientSave struct {
	dynamodbiface.DynamoDBAPI
	items map[string]string
}

func (m *mockDynamoClientSave) PutItem(params *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	shardID := params.Item["ShardID"].S
	checkpoint := params.Item["Checkpoint"].S
	m.items[*shardID] = *checkpoint
	return &dynamodb.PutItemOutput{}, nil
}

func (m *mockDynamoClientSave) GetItem(params *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	shardID := params.Key["ShardID"].S
	checkpoint := m.items[*shardID]
	result := &dynamodb.GetItemOutput{
		Item: map[string]*dynamodb.AttributeValue{
			"ShardID":    {S: shardID},
			"Checkpoint": {S: &checkpoint},
		},
	}
	return result, nil
}

func TestSaveItem(t *testing.T) {
	ss := shardStatus{
		ShardID:    "0000",
		Checkpoint: "11111",
	}
	ms, _ := ss.Marshal()
	svc := &mockDynamoClientSave{
		items: make(map[string]string),
	}

	err := kinesisStateStore.SaveItem(ms, svc)
	if err != nil {
		t.Errorf("SaveItem() error = %v", err)
	}

	savedStatus, _ := kinesisStateStore.GetItem("0000", svc)
	storedStatus, _ := unmarshalShardStatus(savedStatus)

	if !reflect.DeepEqual(storedStatus, ss) {
		t.Errorf("Expected %v, got %v", ss, storedStatus)
	}
}
