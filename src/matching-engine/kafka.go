package main

import (
	"fmt"
	"slices"

	"github.com/IBM/sarama"
)

type KafkaConsumerAdmin interface {
	Partitions(topic string) ([]int32, error)
	Topics() ([]string, error)
	ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error)
}

type KafkaClient struct {
	commonConfig   *sarama.Config
	consumerConfig *sarama.Config
	producerConfig *sarama.Config
	brokers        []string
	adminClient    KafkaConsumerAdmin
}

func NewKafkaClient() *KafkaClient {
	kc := new(KafkaClient)
	kc.brokers = []string{getenv("OB__KAFKA__BOOTSTRAP_SERVERS", "localhost:9094")}
	kc.commonConfig = sarama.NewConfig()
	kc.commonConfig.ClientID = "go-orderbook-consumer"
	kc.commonConfig.Net.SASL.Enable = false
	if getenv("OB__KAFKA__SECURITY_PROTOCOL", "PLAINTEXT") == "PLAINTEXT" {
		kc.commonConfig.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	}

	kc.consumerConfig = sarama.NewConfig()
	kc.consumerConfig.Consumer.Return.Errors = true
	kc.consumerConfig.Consumer.Offsets.Initial = sarama.OffsetNewest

	kc.producerConfig = sarama.NewConfig()
	kc.producerConfig.Producer.Retry.Max = 5
	kc.producerConfig.Producer.RequiredAcks = sarama.WaitForAll
	kc.producerConfig.Producer.Idempotent = true
	kc.producerConfig.Net.MaxOpenRequests = 1
	kc.producerConfig.Producer.Return.Successes = true

	kc.adminClient = kc.GetConsumer()
	return kc
}

func (kc *KafkaClient) GetConsumer() sarama.Consumer {
	consumer, err := sarama.NewConsumer(kc.brokers, kc.consumerConfig)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err != nil {
			panic(err)
		}
	}()
	return consumer
}

func (kc *KafkaClient) GetProducer() *sarama.SyncProducer {
	producer, err := sarama.NewSyncProducer(kc.brokers, kc.producerConfig)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err != nil {
			panic(err)
		}
	}()
	return &producer
}

func (kc *KafkaClient) ConsumerMessagesChan(topic string) (chan *sarama.ConsumerMessage, chan *sarama.ConsumerError) {
	consumers := make(chan *sarama.ConsumerMessage)
	errors := make(chan *sarama.ConsumerError, 1)
	logger := logWithMethod("kafka.consume.setup")

	emitSetupError := func(err error) {
		logger.Error("kafka consumer setup failed", "topic", topic, "error", err)
		errors <- &sarama.ConsumerError{Topic: topic, Partition: -1, Err: err}
		close(consumers)
		close(errors)
	}

	partitions, err := kc.adminClient.Partitions(topic)
	if err != nil {
		emitSetupError(fmt.Errorf("failed to fetch partitions for topic %s: %w", topic, err))
		return consumers, errors
	}
	if len(partitions) == 0 {
		emitSetupError(fmt.Errorf("no partitions found for topic %s", topic))
		return consumers, errors
	}

	topics, err := kc.adminClient.Topics()
	if err != nil {
		emitSetupError(fmt.Errorf("failed to list topics while subscribing to %s: %w", topic, err))
		return consumers, errors
	}

	if !slices.Contains(topics, topic) {
		emitSetupError(fmt.Errorf("topic %s not found", topic))
		return consumers, errors
	}

	logger.Debug("kafka topics discovered", "topics", topics)
	consumer, err := kc.adminClient.ConsumePartition(
		topic,
		partitions[0], // TODO: only first partition for now
		sarama.OffsetOldest,
	)
	if err != nil {
		emitSetupError(fmt.Errorf("failed to consume topic %s partition %d: %w", topic, partitions[0], err))
		return consumers, errors
	}
	logger.Info("kafka consumer subscribed", "topic", topic, "partition", partitions[0])

	go func(topic string, consumer sarama.PartitionConsumer) {
		consumeLogger := logWithMethod("kafka.consume.stream")
		for {
			select {
			case consumerError, ok := <-consumer.Errors():
				if !ok {
					return
				}
				errors <- consumerError
				consumeLogger.Error("kafka consumer stream error", "topic", topic, "partition", consumerError.Partition, "error", consumerError.Err)
			case msg, ok := <-consumer.Messages():
				if !ok {
					return
				}
				consumers <- msg
			}
		}
	}(topic, consumer)

	return consumers, errors
}
