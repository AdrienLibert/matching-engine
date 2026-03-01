package main

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

type fakeKafkaConsumerAdmin struct {
	partitions        []int32
	partitionsErr     error
	topics            []string
	topicsErr         error
	consumeErr        error
	partitionConsumer sarama.PartitionConsumer
}

func (f *fakeKafkaConsumerAdmin) Partitions(topic string) ([]int32, error) {
	if f.partitionsErr != nil {
		return nil, f.partitionsErr
	}
	return f.partitions, nil
}

func (f *fakeKafkaConsumerAdmin) Topics() ([]string, error) {
	if f.topicsErr != nil {
		return nil, f.topicsErr
	}
	return f.topics, nil
}

func (f *fakeKafkaConsumerAdmin) ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	if f.consumeErr != nil {
		return nil, f.consumeErr
	}
	return f.partitionConsumer, nil
}

type fakePartitionConsumer struct {
	messages chan *sarama.ConsumerMessage
	errors   chan *sarama.ConsumerError
}

func newFakePartitionConsumer(buffer int) *fakePartitionConsumer {
	return &fakePartitionConsumer{
		messages: make(chan *sarama.ConsumerMessage, buffer),
		errors:   make(chan *sarama.ConsumerError, buffer),
	}
}

func (f *fakePartitionConsumer) AsyncClose() {
	close(f.messages)
	close(f.errors)
}

func (f *fakePartitionConsumer) Close() error {
	f.AsyncClose()
	return nil
}

func (f *fakePartitionConsumer) Messages() <-chan *sarama.ConsumerMessage {
	return f.messages
}

func (f *fakePartitionConsumer) Errors() <-chan *sarama.ConsumerError {
	return f.errors
}

func (f *fakePartitionConsumer) HighWaterMarkOffset() int64 {
	return 0
}

func (f *fakePartitionConsumer) Pause() {}

func (f *fakePartitionConsumer) Resume() {}

func (f *fakePartitionConsumer) IsPaused() bool {
	return false
}

func TestConsumerMessagesChanEmptyPartitionsReturnsExplicitErrorNoPanic(t *testing.T) {
	client := &KafkaClient{
		adminClient: &fakeKafkaConsumerAdmin{
			partitions: []int32{},
			topics:     []string{"orders.topic"},
		},
	}

	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("ConsumerMessagesChan panicked on empty partitions: %v", recovered)
		}
	}()

	messages, consumerErrors := client.ConsumerMessagesChan("orders.topic")

	select {
	case consumerError, ok := <-consumerErrors:
		if !ok {
			t.Fatalf("expected setup error, got closed error channel")
		}
		if consumerError == nil || consumerError.Err == nil {
			t.Fatalf("expected non-nil setup error for empty partitions")
		}
		if !strings.Contains(consumerError.Err.Error(), "no partitions found for topic orders.topic") {
			t.Fatalf("unexpected setup error: %v", consumerError.Err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for setup error on empty partitions")
	}

	if _, ok := <-messages; ok {
		t.Fatalf("expected message channel to be closed on setup failure")
	}
}

func TestConsumerMessagesChanMissingTopicReturnsExplicitErrorNoPanic(t *testing.T) {
	client := &KafkaClient{
		adminClient: &fakeKafkaConsumerAdmin{
			partitions: []int32{0},
			topics:     []string{"another.topic"},
		},
	}

	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("ConsumerMessagesChan panicked on missing topic: %v", recovered)
		}
	}()

	messages, consumerErrors := client.ConsumerMessagesChan("orders.topic")

	select {
	case consumerError, ok := <-consumerErrors:
		if !ok {
			t.Fatalf("expected setup error, got closed error channel")
		}
		if consumerError == nil || consumerError.Err == nil {
			t.Fatalf("expected non-nil setup error for missing topic")
		}
		if !strings.Contains(consumerError.Err.Error(), "topic orders.topic not found") {
			t.Fatalf("unexpected setup error: %v", consumerError.Err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for setup error on missing topic")
	}

	if _, ok := <-messages; ok {
		t.Fatalf("expected message channel to be closed on setup failure")
	}
}

func TestConsumerMessagesChanForwardsConsumerErrorsAndMessagesNoPanic(t *testing.T) {
	partitionConsumer := newFakePartitionConsumer(2)
	client := &KafkaClient{
		adminClient: &fakeKafkaConsumerAdmin{
			partitions:        []int32{0},
			topics:            []string{"orders.topic"},
			partitionConsumer: partitionConsumer,
		},
	}

	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("ConsumerMessagesChan panicked while forwarding messages/errors: %v", recovered)
		}
	}()

	messages, consumerErrors := client.ConsumerMessagesChan("orders.topic")

	expectedConsumerError := &sarama.ConsumerError{Topic: "orders.topic", Partition: 0, Err: errors.New("consumer failed")}
	partitionConsumer.errors <- expectedConsumerError

	select {
	case forwardedError := <-consumerErrors:
		if forwardedError == nil || forwardedError.Err == nil {
			t.Fatalf("expected forwarded consumer error")
		}
		if forwardedError.Topic != "orders.topic" || forwardedError.Partition != 0 {
			t.Fatalf("unexpected forwarded consumer error metadata: %+v", forwardedError)
		}
		if forwardedError.Err.Error() != "consumer failed" {
			t.Fatalf("unexpected forwarded consumer error: %v", forwardedError.Err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for forwarded consumer error")
	}

	expectedMessage := &sarama.ConsumerMessage{Topic: "orders.topic", Partition: 0, Value: []byte("payload")}
	partitionConsumer.messages <- expectedMessage

	select {
	case forwardedMessage := <-messages:
		if forwardedMessage == nil {
			t.Fatalf("expected forwarded consumer message")
		}
		if forwardedMessage.Topic != "orders.topic" || forwardedMessage.Partition != 0 || string(forwardedMessage.Value) != "payload" {
			t.Fatalf("unexpected forwarded message: %+v", forwardedMessage)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for forwarded consumer message")
	}
}
