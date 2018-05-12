package input

import (
	"github.com/patrobinson/gokini"
	log "github.com/sirupsen/logrus"
)

type KinesisConfig struct {
	StreamName string `json:"streamName"`
}

// KinesisInput implements the Input interface
type KinesisInput struct {
	outputChan      *chan []byte
	StreamName      string
	recordConsumer  *recordConsumer
	kinesisConsumer *gokini.KinesisConsumer
}

type recordConsumer struct {
	shardID    string
	outputChan *chan []byte
}

func (p *recordConsumer) Init(shardID string) error {
	log.Debugln("Consumer initializing", shardID)
	p.shardID = shardID
	return nil
}

func (p *recordConsumer) ProcessRecords(records []*gokini.Records, consumer *gokini.KinesisConsumer) {
	for _, record := range records {
		*p.outputChan <- record.Data
	}
}

func (p *recordConsumer) Shutdown() {
	log.Debugln("Consumer Shutdown", p.shardID)
}

const tableName = "GoFish"

// Init implements initialises the Input mechanism
func (ki *KinesisInput) Init() error {
	ki.kinesisConsumer = &gokini.KinesisConsumer{
		StreamName:           ki.StreamName,
		ShardIteratorType:    "TRIM_HORIZON",
		RecordConsumer:       ki.recordConsumer,
		TableName:            tableName,
		EmptyRecordBackoffMs: 1000,
	}
	return nil
}

// Retrieve implements the Input interface
func (ki *KinesisInput) Retrieve(output *chan []byte) {
	ki.recordConsumer = &recordConsumer{
		outputChan: output,
	}
	err := ki.kinesisConsumer.StartConsumer()
	if err != nil {
		log.Fatalln("Failed to start consumer:", err)
	}
}
