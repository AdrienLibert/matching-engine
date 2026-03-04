package main

import (
	"fmt"
	"orderbookpb/contracts"
	"slices"

	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"
)

type KafkaClient struct {
	commonConfig    *sarama.Config
	consumerConfig  *sarama.Config
	producerConfig  *sarama.Config
	brokers         []string
	tradeTopic      string
	quoteTopic      string
	pricePointTopic string
}

func NewKafkaClient() *KafkaClient {
	kc := new(KafkaClient)
	kc.brokers = []string{getenv("OB__KAFKA__BOOTSTRAP_SERVERS", "localhost:9094")}
	kc.commonConfig = sarama.NewConfig()
	kc.commonConfig.ClientID = "go-traderpool-consumer"
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

	kc.pricePointTopic = "order.last_price.topic"
	kc.tradeTopic = "trades.topic"
	kc.quoteTopic = "orders.topic"
	return kc
}

func (kc *KafkaClient) GetConsumer() *sarama.Consumer {
	consumer, err := sarama.NewConsumer(kc.brokers, kc.consumerConfig)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err != nil {
			panic(err)
		}
	}()
	return &consumer
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

func (kc *KafkaClient) Assign(master sarama.Consumer, topic string) (chan *sarama.ConsumerMessage, chan *sarama.ConsumerError) {
	consumers := make(chan *sarama.ConsumerMessage)
	errors := make(chan *sarama.ConsumerError, 1)

	partitions, err := master.Partitions(topic)
	if err != nil {
		errors <- &sarama.ConsumerError{Topic: topic, Err: fmt.Errorf("unable to list partitions for topic %s: %w", topic, err)}
		close(consumers)
		close(errors)
		return consumers, errors
	}

	topics, err := master.Topics()
	if err != nil {
		errors <- &sarama.ConsumerError{Topic: topic, Err: fmt.Errorf("unable to list topics: %w", err)}
		close(consumers)
		close(errors)
		return consumers, errors
	}
	fmt.Println("DEBUG: topics: ", topics)

	if !slices.Contains(topics, topic) {
		errors <- &sarama.ConsumerError{Topic: topic, Err: fmt.Errorf("topic %s not found", topic)}
		close(consumers)
		close(errors)
		return consumers, errors
	}

	if len(partitions) == 0 {
		errors <- &sarama.ConsumerError{Topic: topic, Err: fmt.Errorf("no partitions found for topic %s", topic)}
		close(consumers)
		close(errors)
		return consumers, errors
	}

	consumer, err := master.ConsumePartition(
		topic,
		partitions[0], // TODO: only first partition for now
		sarama.OffsetOldest,
	)

	if err != nil {
		fmt.Printf("ERROR: topic %v partitions %v", topic, partitions)
		errors <- &sarama.ConsumerError{Topic: topic, Partition: partitions[0], Err: fmt.Errorf("unable to consume partition for topic %s: %w", topic, err)}
		close(consumers)
		close(errors)
		return consumers, errors
	}

	go func(topic string, consumer sarama.PartitionConsumer) {
		defer close(consumers)
		defer close(errors)
		for {
			select {
			case consumerError, ok := <-consumer.Errors():
				if !ok {
					return
				}
				errors <- consumerError
				fmt.Println("ERROR: not able to consume: ", consumerError.Err)
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

func handleError(err error) {
	fmt.Println("ERROR: invalid message consumed:", err)
}

func convertOrderToMessage(order Order) []byte {
	message, err := proto.Marshal(toProtoOrder(order))
	if err != nil {
		fmt.Println("ERROR: invalid order being converted to protobuf message:", err)
	}
	return message
}

func convertMessageToTrade(messageValue []byte) (Trade, error) {
	var wireTrade contracts.Trade
	if err := proto.Unmarshal(messageValue, &wireTrade); err != nil {
		return Trade{}, err
	}

	return tradeFromProto(&wireTrade), nil
}

func convertMessageToPricePoint(value []byte) (PricePoint, error) {
	var wirePricePoint contracts.PricePoint
	err := proto.Unmarshal(value, &wirePricePoint)
	if err != nil {
		return PricePoint{}, err
	}

	return pricePointFromProto(&wirePricePoint), nil
}

func toProtoOrder(order Order) *contracts.Order {
	return &contracts.Order{
		OrderId:   order.OrderID,
		OrderType: order.OrderType,
		Price:     order.Price,
		Quantity:  order.Quantity,
		Action:    order.Action,
		Timestamp: order.Timestamp,
	}
}

func tradeFromProto(trade *contracts.Trade) Trade {
	if trade == nil {
		return Trade{}
	}

	return Trade{
		TradeId:   trade.TradeId,
		OrderId:   trade.OrderId,
		Quantity:  trade.Quantity,
		Price:     trade.Price,
		Action:    trade.Action,
		Status:    trade.Status,
		Timestamp: trade.Timestamp,
	}
}

func pricePointFromProto(pricePoint *contracts.PricePoint) PricePoint {
	if pricePoint == nil {
		return PricePoint{}
	}

	return PricePoint{Price: pricePoint.Price}
}
