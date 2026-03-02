package main

import (
	"errors"
	"math"
	"orderbookpb/contracts"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"
)

type fakeConsumer struct {
	partitions        []int32
	partitionsErr     error
	topics            []string
	topicsErr         error
	consumeErr        error
	partitionConsumer sarama.PartitionConsumer
}

func (f *fakeConsumer) Topics() ([]string, error) {
	if f.topicsErr != nil {
		return nil, f.topicsErr
	}
	return f.topics, nil
}

func (f *fakeConsumer) Partitions(topic string) ([]int32, error) {
	if f.partitionsErr != nil {
		return nil, f.partitionsErr
	}
	return f.partitions, nil
}

func (f *fakeConsumer) ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	if f.consumeErr != nil {
		return nil, f.consumeErr
	}
	return f.partitionConsumer, nil
}

func (f *fakeConsumer) HighWaterMarks() map[string]map[int32]int64 {
	return map[string]map[int32]int64{}
}

func (f *fakeConsumer) Close() error {
	return nil
}

func (f *fakeConsumer) Pause(topicPartitions map[string][]int32) {}

func (f *fakeConsumer) Resume(topicPartitions map[string][]int32) {}

func (f *fakeConsumer) PauseAll() {}

func (f *fakeConsumer) ResumeAll() {}

type fakePartitionConsumer struct {
	messages chan *sarama.ConsumerMessage
	errors   chan *sarama.ConsumerError
	close    sync.Once
}

func newFakePartitionConsumer(buffer int) *fakePartitionConsumer {
	return &fakePartitionConsumer{
		messages: make(chan *sarama.ConsumerMessage, buffer),
		errors:   make(chan *sarama.ConsumerError, buffer),
	}
}

func (f *fakePartitionConsumer) AsyncClose() {
	f.close.Do(func() {
		close(f.messages)
		close(f.errors)
	})
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

func TestConvertOrderToMessageRoundTrip(t *testing.T) {
	order := Order{
		OrderID:   "Trader-12-1",
		OrderType: "limit",
		Price:     102.51,
		Quantity:  30,
		Action:    "BUY",
		Timestamp: 1700000000,
	}

	raw := convertOrderToMessage(order)

	var got contracts.Order
	if err := proto.Unmarshal(raw, &got); err != nil {
		t.Fatalf("expected valid protobuf order message, got error: %v", err)
	}

	if got.OrderId != order.OrderID ||
		got.OrderType != order.OrderType ||
		got.Price != order.Price ||
		got.Quantity != order.Quantity ||
		got.Action != order.Action ||
		got.Timestamp != order.Timestamp {
		t.Fatalf("round-trip mismatch, got %+v want %+v", got, order)
	}
}

func TestConvertMessageToTradeScenarios(t *testing.T) {
	tests := []struct {
		name      string
		payload   []byte
		wantErr   bool
		assertion func(t *testing.T, trade Trade)
	}{
		{
			name:    "valid trade payload",
			payload: func() []byte {
				msg, err := proto.Marshal(&contracts.Trade{
					TradeId:   "t1",
					OrderId:   "o1",
					Quantity:  5,
					Price:     101.1,
					Action:    "BUY",
					Status:    "FILLED",
					Timestamp: 9,
				})
				if err != nil {
					t.Fatalf("failed to marshal protobuf trade message: %v", err)
				}
				return msg
			}(),
			assertion: func(t *testing.T, trade Trade) {
				t.Helper()
				if trade.TradeId != "t1" || trade.OrderId != "o1" || trade.Quantity != 5 || math.Abs(trade.Price-101.1) > 1e-9 || trade.Action != "BUY" || trade.Status != "FILLED" || trade.Timestamp != 9 {
					t.Fatalf("unexpected trade: %+v", trade)
				}
			},
		},
		{
			name:    "invalid trade protobuf",
			payload: []byte(`{"trade_id":"broken"`),
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			trade, err := convertMessageToTrade(tc.payload)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if tc.assertion != nil {
				tc.assertion(t, trade)
			}
		})
	}
}

func TestConvertMessageToPricePointScenarios(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte
		wantErr bool
		want    float64
	}{
		{name: "valid price point", payload: func() []byte {
			msg, err := proto.Marshal(&contracts.PricePoint{Price: 49.75})
			if err != nil {
				t.Fatalf("failed to marshal protobuf price point message: %v", err)
			}
			return msg
		}(), want: 49.75},
		{name: "invalid price point", payload: []byte(`{"price":`), wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pp, err := convertMessageToPricePoint(tc.payload)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
			if pp.Price != tc.want {
				t.Fatalf("unexpected price point: got %.2f want %.2f", pp.Price, tc.want)
			}
		})
	}
}

func TestAssignEmptyPartitionsReturnsExplicitError(t *testing.T) {
	kc := &KafkaClient{}
	fakeMaster := &fakeConsumer{
		partitions: []int32{},
		topics:     []string{"trades.topic"},
	}

	messages, consumerErrors := kc.Assign(fakeMaster, "trades.topic")

	select {
	case consumerError, ok := <-consumerErrors:
		if !ok {
			t.Fatalf("expected setup error, got closed channel")
		}
		if consumerError == nil || consumerError.Err == nil {
			t.Fatalf("expected non-nil setup error")
		}
		if !strings.Contains(consumerError.Err.Error(), "no partitions found for topic trades.topic") {
			t.Fatalf("unexpected error: %v", consumerError.Err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for setup error")
	}

	if _, ok := <-messages; ok {
		t.Fatalf("expected messages channel to be closed")
	}
}

func TestAssignPartitionsFailureReturnsExplicitError(t *testing.T) {
	kc := &KafkaClient{}
	fakeMaster := &fakeConsumer{
		partitionsErr: errors.New("partitions unavailable"),
	}

	messages, consumerErrors := kc.Assign(fakeMaster, "trades.topic")

	select {
	case consumerError, ok := <-consumerErrors:
		if !ok {
			t.Fatalf("expected setup error, got closed channel")
		}
		if consumerError == nil || consumerError.Err == nil {
			t.Fatalf("expected non-nil setup error")
		}
		if !strings.Contains(consumerError.Err.Error(), "unable to list partitions for topic trades.topic") {
			t.Fatalf("unexpected error: %v", consumerError.Err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for setup error")
	}

	if _, ok := <-messages; ok {
		t.Fatalf("expected messages channel to be closed")
	}
}

func TestAssignTopicsFailureReturnsExplicitError(t *testing.T) {
	kc := &KafkaClient{}
	fakeMaster := &fakeConsumer{
		partitions: []int32{0},
		topicsErr:   errors.New("topics unavailable"),
	}

	messages, consumerErrors := kc.Assign(fakeMaster, "trades.topic")

	select {
	case consumerError, ok := <-consumerErrors:
		if !ok {
			t.Fatalf("expected setup error, got closed channel")
		}
		if consumerError == nil || consumerError.Err == nil {
			t.Fatalf("expected non-nil setup error")
		}
		if !strings.Contains(consumerError.Err.Error(), "unable to list topics") {
			t.Fatalf("unexpected error: %v", consumerError.Err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for setup error")
	}

	if _, ok := <-messages; ok {
		t.Fatalf("expected messages channel to be closed")
	}
}

func TestAssignMissingTopicReturnsExplicitError(t *testing.T) {
	kc := &KafkaClient{}
	fakeMaster := &fakeConsumer{
		partitions: []int32{0},
		topics:     []string{"other.topic"},
	}

	messages, consumerErrors := kc.Assign(fakeMaster, "trades.topic")

	select {
	case consumerError, ok := <-consumerErrors:
		if !ok {
			t.Fatalf("expected setup error, got closed channel")
		}
		if consumerError == nil || consumerError.Err == nil {
			t.Fatalf("expected non-nil setup error")
		}
		if !strings.Contains(consumerError.Err.Error(), "topic trades.topic not found") {
			t.Fatalf("unexpected error: %v", consumerError.Err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for setup error")
	}

	if _, ok := <-messages; ok {
		t.Fatalf("expected messages channel to be closed")
	}
}

func TestAssignForwardsMessagesAndErrors(t *testing.T) {
	fakePartition := newFakePartitionConsumer(2)
	kc := &KafkaClient{}
	fakeMaster := &fakeConsumer{
		partitions:        []int32{0},
		topics:            []string{"trades.topic"},
		partitionConsumer: fakePartition,
	}

	messages, consumerErrors := kc.Assign(fakeMaster, "trades.topic")

	expectedErr := &sarama.ConsumerError{Topic: "trades.topic", Partition: 0, Err: errors.New("consumer failed")}
	fakePartition.errors <- expectedErr

	select {
	case gotErr := <-consumerErrors:
		if gotErr == nil || gotErr.Err == nil {
			t.Fatalf("expected forwarded error")
		}
		if gotErr.Topic != "trades.topic" || gotErr.Partition != 0 || gotErr.Err.Error() != "consumer failed" {
			t.Fatalf("unexpected forwarded error: %+v", gotErr)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for forwarded error")
	}

	expectedMsg := &sarama.ConsumerMessage{Topic: "trades.topic", Partition: 0, Value: []byte("payload")}
	fakePartition.messages <- expectedMsg

	select {
	case gotMsg := <-messages:
		if gotMsg == nil {
			t.Fatalf("expected forwarded message")
		}
		if gotMsg.Topic != "trades.topic" || gotMsg.Partition != 0 || string(gotMsg.Value) != "payload" {
			t.Fatalf("unexpected forwarded message: %+v", gotMsg)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for forwarded message")
	}

	fakePartition.AsyncClose()
}

func TestAssignConsumePartitionFailureReturnsExplicitError(t *testing.T) {
	kc := &KafkaClient{}
	fakeMaster := &fakeConsumer{
		partitions: []int32{0},
		topics:     []string{"trades.topic"},
		consumeErr: errors.New("consume partition failed"),
	}

	messages, consumerErrors := kc.Assign(fakeMaster, "trades.topic")

	select {
	case consumerError, ok := <-consumerErrors:
		if !ok {
			t.Fatalf("expected setup error, got closed channel")
		}
		if consumerError == nil || consumerError.Err == nil {
			t.Fatalf("expected non-nil setup error")
		}
		if !strings.Contains(consumerError.Err.Error(), "unable to consume partition for topic trades.topic") {
			t.Fatalf("unexpected error: %v", consumerError.Err)
		}
		if consumerError.Partition != 0 {
			t.Fatalf("expected partition 0, got %d", consumerError.Partition)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for setup error")
	}

	if _, ok := <-messages; ok {
		t.Fatalf("expected messages channel to be closed")
	}
}
