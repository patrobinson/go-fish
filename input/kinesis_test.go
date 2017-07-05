package input

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
)

func setupInput(mockClient kinesisiface.KinesisAPI) *KinesisInput {
	outChan := make(chan []byte)
	mgmtChan := make(chan shardChange)
	return &KinesisInput{
		outputChan: &outChan,
		shardMgmt:  &mgmtChan,
		StreamName: "testStream",
		kinesisSvc: mockClient,
		shardIds: map[string]shardStatus{
			"00000001": shardStatus{
				shardID: "00000001",
			},
		},
	}
}

type mockKClientGet struct {
	kinesisiface.KinesisAPI
}

func (k *mockKClientGet) GetShardIterator(args *kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
	return &kinesis.GetShardIteratorOutput{
		ShardIterator: aws.String("0123456789ABCDEF"),
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

func TestShardClosed(t *testing.T) {
	input := setupInput(&mockKClientClosedShard{})
	inputC := make(chan []byte)
	go input.shardIDManager()
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

func TestGetShardIds(t *testing.T) {
	client := &mockKClientGetShards{}
	shards, err := getShardIds(client, "testStream", "")
	if err != nil {
		t.Errorf("Error getting Shards: %v", err)
	}
	if v, ok := shards["00000001"]; !ok || v.shardID != "00000001" {
		t.Errorf("Invalid Shard ID returned")
	}
	if len(shards) != 1 {
		t.Errorf("Too many shards")
	}
}
