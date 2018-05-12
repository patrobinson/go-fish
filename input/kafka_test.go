package input

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	log "github.com/sirupsen/logrus"
)

func setupKafka(topic string, t *testing.T) *KafkaInput {
	consumer := mocks.NewConsumer(t, nil)
	input := &KafkaInput{
		Broker:     "foo",
		Topic:      topic,
		Partitions: 2,
		consumer:   consumer,
	}
	consumer.ExpectConsumePartition(topic, 0, sarama.OffsetNewest).YieldMessage(&sarama.ConsumerMessage{Value: []byte("hello partition 0")})
	consumer.ExpectConsumePartition(topic, 1, sarama.OffsetNewest).YieldMessage(&sarama.ConsumerMessage{Value: []byte("hello partition 1")})
	return input
}

func TestKafkaInput_CreatePartitionConsumers(t *testing.T) {
	topic := "bar"
	input := setupKafka(topic, t)
	err := input.createPartitionConsumers()
	if err != nil {
		t.Fatalf("Failed to create partitions: %s", err)
	}
	if len(input.partConsumers) != 2 {
		t.Fatalf("Expected 2 partition consumers, got %d", len(input.partConsumers))
	}
}

func TestKafkaInput_Retrieve(t *testing.T) {
	topic := "test"
	input := setupKafka(topic, t)
	err := input.createPartitionConsumers()
	if err != nil {
		t.Fatalf("Failed to create partitions: %s", err)
	}
	output := make(chan []byte)
	input.Retrieve(&output)
	// LIFO
	if msg := <-output; string(msg) != "hello partition 1" {
		log.Fatalf("Expected message 'hellow partition 1', got %s", msg)
	}
	if msg := <-output; string(msg) != "hello partition 0" {
		log.Fatalf("Expected message 'hellow partition 0', got %s", msg)
	}
}
