package input

import (
	"fmt"

	"github.com/Shopify/sarama"
	log "github.com/sirupsen/logrus"
)

type KafkaConfig struct {
	Broker     string `json:"broker"`
	Topic      string `json:"topic"`
	Partitions int32  `json:"partitions"`
}

type KafkaInput struct {
	Broker        string
	Topic         string
	Partitions    int32
	consumer      sarama.Consumer
	partConsumers []*sarama.PartitionConsumer
	outputChan    *chan interface{}
}

func (k *KafkaInput) Init(...interface{}) error {
	var err error
	k.consumer, err = sarama.NewConsumer([]string{k.Broker}, nil)
	if err != nil {
		log.Errorf("Unable to open Consumer: %v", err)
	}
	return k.createPartitionConsumers()
}

func (k *KafkaInput) createPartitionConsumers() error {
	for i := int32(0); i < k.Partitions; i++ {
		partitionConsumer, err := k.consumer.ConsumePartition(k.Topic, i, sarama.OffsetNewest)
		if err != nil {
			log.Errorf("Unable to create partition consumer for topic %v partition %v: %v", k.Topic, i, err)
			return err
		}
		k.partConsumers = append(k.partConsumers, &partitionConsumer)
	}
	return nil
}

func (k *KafkaInput) Retrieve(output *chan interface{}) {
	k.outputChan = output
	for _, partitionConsumer := range k.partConsumers {
		go k.getMessages(partitionConsumer)
	}
}

func (k *KafkaInput) getMessages(partConsumer *sarama.PartitionConsumer) {
	for {
		msg := <-(*partConsumer).Messages()
		*k.outputChan <- msg.Value
	}
}

func (k *KafkaInput) Close() error {
	err := k.consumer.Close()
	if err != nil {
		return fmt.Errorf("Failed to close Kafka consumer: %v", err)
	}
	for _, partitionConsumer := range k.partConsumers {
		err = (*partitionConsumer).Close()
		if err != nil {
			return fmt.Errorf("Failed to close Kafka Partition Consumer: %v", err)
		}
	}
	return nil
}
