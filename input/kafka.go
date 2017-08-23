package input

import (
	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
)

type KafkaInput struct {
	Broker        string
	Topic         string
	Partitions    int32
	consumer      sarama.Consumer
	partConsumers []*sarama.PartitionConsumer
}

func (k *KafkaInput) Init() error {
	var err error
	k.consumer, err = sarama.NewConsumer([]string{k.Broker}, nil)
	if err != nil {
		log.Errorf("Unable to open Consumer: %v", err)
	}
	for i := int32(0); i <= k.Partitions; i++ {
		partitionConsumer, err := k.consumer.ConsumePartition(k.Topic, i, sarama.OffsetNewest)
		if err != nil {
			log.Errorf("Unable to create partition consumer: %v", err)
			return err
		}
		k.partConsumers = append(k.partConsumers, &partitionConsumer)
	}
	return nil
}

func (k *KafkaInput) Retrieve(output *chan []byte) {
	for {
		for _, partitionConsumer := range k.partConsumers {
			msg := <-(*partitionConsumer).Messages()
			*output <- msg.Value
		}
	}
}

func (k *KafkaInput) Close() {
	err := k.consumer.Close()
	if err != nil {
		log.Errorf("Failed to close Kafka consumer: %v", err)
	}
	for _, partitionConsumer := range k.partConsumers {
		err = (*partitionConsumer).Close()
		if err != nil {
			log.Errorf("Failed to close Kafka Partition Consumer: %v", err)
		}
	}
}
