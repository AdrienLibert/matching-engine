package main

import (
	"container/heap"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMinHeapPop(t *testing.T) {
	h := &MinHeap{2.0, 1.0, 5.0, 0.0, 6.0, 7.0}
	wants := []float64{0.0, 1.0, 2.0, 5.0, 6.0, 7.0}
	heap.Init(h)

	// Test pop
	count := 0
	for h.Len() > 0 {
		got := heap.Pop(h)
		want := wants[count]
		if got != want {
			t.Errorf("got %f, wanted %f for index %d", got, want, count)
		}
		count++
	}
}

func TestMinHeapPeak(t *testing.T) {
	h := &MinHeap{2.0, 1.0, 5.0, 0.0, 6.0, 7.0}
	wants := []float64{0.0, 1.0, 2.0, 5.0, 6.0, 7.0}
	heap.Init(h)

	// Test pop
	count := 0
	for h.Len() > 0 {
		got := h.Peak().(float64)
		want := wants[count]
		if got != want {
			t.Errorf("got %f, wanted %f for index %d", got, want, count)
		}
		count++
		heap.Pop(h)
	}
}

func TestMaxHeapPop(t *testing.T) {
	h := &MaxHeap{2.0, 1.0, 5.0, 0.0, 6.0, 7.0}
	wants := []float64{7.0, 6.0, 5.0, 2.0, 1.0, 0.0}
	heap.Init(h)

	// Test pop
	count := 0
	for h.Len() > 0 {
		got := heap.Pop(h)
		want := wants[count]
		if got != want {
			t.Errorf("got %f, wanted %f for index %d", got, want, count)
		}
		count++
	}
}

func TestMaxHeapPeak(t *testing.T) {
	h := &MaxHeap{2.0, 1.0, 5.0, 0.0, 6.0, 7.0}
	wants := []float64{7.0, 6.0, 5.0, 2.0, 1.0, 0.0}
	heap.Init(h)

	// Test pop
	count := 0
	for h.Len() > 0 {
		got := h.Peak().(float64)
		want := wants[count]
		if got != want {
			t.Errorf("got %f, wanted %f for index %d", got, want, count)
		}
		count++
		heap.Pop(h)
	}
}

func TestOrderBookAddOrder(t *testing.T) {
	orderbook := NewOrderBook()
	now := time.Now().UTC().Unix()
	price := 10.0
	buyOrder := Order{
		OrderID:   "uuid-uuid-uuid-uuid",
		OrderType: "limit",
		Price:     price,
		Quantity:  20,
		Timestamp: now,
	}

	orderbook.AddOrder(&buyOrder, "BUY")
	assert.Equal(t, 1, orderbook.BestBid.Len())
	assert.Equal(t, price, orderbook.BestBid.Peak())
	assert.Equal(t, 1, len(orderbook.PriceToBuyOrders))
	assert.Equal(t, 1, orderbook.OpenOrderCount())

	sellOrder := Order{
		OrderID:   "uuid-uuid-uuid-uuid",
		OrderType: "limit",
		Price:     price,
		Quantity:  20,
		Timestamp: now,
	}
	orderbook.AddOrder(&sellOrder, "SELL")
	assert.Equal(t, 1, orderbook.BestAsk.Len())
	assert.Equal(t, price, orderbook.BestAsk.Peak())
	assert.Equal(t, 1, len(orderbook.PriceToSellOrders))
	assert.Equal(t, 2, orderbook.OpenOrderCount())

	assert.Equal(t, 1, orderbook.BestBid.Len())
	assert.Equal(t, price, orderbook.BestBid.Peak())
	assert.Equal(t, 1, len(orderbook.PriceToBuyOrders))
}

func TestOrderBookOpenOrderCountTracksMutations(t *testing.T) {
	orderbook := NewOrderBook()
	now := time.Now().UTC().Unix()

	buy1 := Order{OrderID: "buy-1", OrderType: "limit", Price: 10.0, Quantity: 2, Timestamp: now}
	buy2 := Order{OrderID: "buy-2", OrderType: "limit", Price: 10.0, Quantity: 3, Timestamp: now}
	sell1 := Order{OrderID: "sell-1", OrderType: "limit", Price: 11.0, Quantity: 4, Timestamp: now}

	orderbook.AddOrder(&buy1, "BUY")
	orderbook.AddOrder(&buy2, "BUY")
	orderbook.AddOrder(&sell1, "SELL")
	assert.Equal(t, 3, orderbook.OpenOrderCount())

	orderbook.decrementOpenOrderCount()
	assert.Equal(t, 2, orderbook.OpenOrderCount())

	orderbook.decrementOpenOrderCount()
	orderbook.decrementOpenOrderCount()
	orderbook.decrementOpenOrderCount()
	assert.Equal(t, 0, orderbook.OpenOrderCount())
}

func TestOrderBookAddOrderBestBidInvariant(t *testing.T) {
	orderbook := NewOrderBook()
	now := time.Now().UTC().Unix()

	type step struct {
		order       Order
		expectedTop float64
		expectedLen int
	}

	steps := []step{
		{
			order:       Order{OrderID: "bid-10", OrderType: "limit", Price: 10.0, Quantity: 1, Timestamp: now},
			expectedTop: 10.0,
			expectedLen: 1,
		},
		{
			order:       Order{OrderID: "bid-9", OrderType: "limit", Price: 9.0, Quantity: 1, Timestamp: now},
			expectedTop: 10.0,
			expectedLen: 2,
		},
		{
			order:       Order{OrderID: "bid-12", OrderType: "limit", Price: 12.0, Quantity: 1, Timestamp: now},
			expectedTop: 12.0,
			expectedLen: 3,
		},
		{
			order:       Order{OrderID: "bid-11", OrderType: "limit", Price: 11.0, Quantity: 1, Timestamp: now},
			expectedTop: 12.0,
			expectedLen: 4,
		},
		{
			order:       Order{OrderID: "bid-12-dup", OrderType: "limit", Price: 12.0, Quantity: 2, Timestamp: now},
			expectedTop: 12.0,
			expectedLen: 4,
		},
	}

	for _, currentStep := range steps {
		order := currentStep.order
		orderbook.AddOrder(&order, "BUY")

		assert.Equal(t, currentStep.expectedTop, orderbook.BestBid.Peak())
		assert.Equal(t, currentStep.expectedLen, len(orderbook.PriceToBuyOrders))
	}
}

func TestOrderBookAddOrderBestAskInvariant(t *testing.T) {
	orderbook := NewOrderBook()
	now := time.Now().UTC().Unix()

	type step struct {
		order       Order
		expectedTop float64
		expectedLen int
	}

	steps := []step{
		{
			order:       Order{OrderID: "ask-12", OrderType: "limit", Price: 12.0, Quantity: 1, Timestamp: now},
			expectedTop: 12.0,
			expectedLen: 1,
		},
		{
			order:       Order{OrderID: "ask-15", OrderType: "limit", Price: 15.0, Quantity: 1, Timestamp: now},
			expectedTop: 12.0,
			expectedLen: 2,
		},
		{
			order:       Order{OrderID: "ask-10", OrderType: "limit", Price: 10.0, Quantity: 1, Timestamp: now},
			expectedTop: 10.0,
			expectedLen: 3,
		},
		{
			order:       Order{OrderID: "ask-11", OrderType: "limit", Price: 11.0, Quantity: 1, Timestamp: now},
			expectedTop: 10.0,
			expectedLen: 4,
		},
		{
			order:       Order{OrderID: "ask-10-dup", OrderType: "limit", Price: 10.0, Quantity: 2, Timestamp: now},
			expectedTop: 10.0,
			expectedLen: 4,
		},
	}

	for _, currentStep := range steps {
		order := currentStep.order
		orderbook.AddOrder(&order, "SELL")

		assert.Equal(t, currentStep.expectedTop, orderbook.BestAsk.Peak())
		assert.Equal(t, currentStep.expectedLen, len(orderbook.PriceToSellOrders))
	}
}

func TestOrderQueuePeekAndPopFIFO(t *testing.T) {
	now := time.Now().UTC().Unix()
	queue := NewOrderQueue()
	first := &Order{OrderID: "first", OrderType: "limit", Price: 10.0, Quantity: 2, Timestamp: now}
	second := &Order{OrderID: "second", OrderType: "limit", Price: 10.0, Quantity: 3, Timestamp: now}

	queue.Push(first)
	queue.Push(second)

	front := queue.PeekFront()
	if assert.NotNil(t, front) {
		assert.Equal(t, "first", front.OrderID)
	}
	assert.Equal(t, 2, queue.Len())

	popped, ok := queue.PopFront()
	assert.True(t, ok)
	if assert.NotNil(t, popped) {
		assert.Equal(t, "first", popped.OrderID)
	}
	assert.Equal(t, 1, queue.Len())

	front = queue.PeekFront()
	if assert.NotNil(t, front) {
		assert.Equal(t, "second", front.OrderID)
	}
}

func TestOrderQueueCompactsAndPreservesOrder(t *testing.T) {
	now := time.Now().UTC().Unix()
	queue := NewOrderQueue()

	for index := 0; index < 128; index++ {
		queue.Push(&Order{OrderID: "order", OrderType: "limit", Price: 10.0, Quantity: 1, Timestamp: now})
	}

	for index := 0; index < 64; index++ {
		_, ok := queue.PopFront()
		assert.True(t, ok)
	}

	assert.Equal(t, 64, queue.Len())
	assert.Equal(t, 0, queue.head)
	assert.Equal(t, 64, len(queue.items))
}

func TestOrderbookCancelOrderRemovesHeadAndPreservesFIFO(t *testing.T) {
	orderbook := NewOrderBook()
	now := time.Now().UTC().Unix()

	first := Order{OrderID: "buy-1", OrderType: "limit", Price: 10.0, Quantity: 2, Timestamp: now}
	second := Order{OrderID: "buy-2", OrderType: "limit", Price: 10.0, Quantity: 3, Timestamp: now}
	third := Order{OrderID: "buy-3", OrderType: "limit", Price: 10.0, Quantity: 4, Timestamp: now}

	orderbook.AddOrder(&first, "BUY")
	orderbook.AddOrder(&second, "BUY")
	orderbook.AddOrder(&third, "BUY")

	removed := orderbook.CancelOrder("buy-1")
	assert.True(t, removed)
	assert.Equal(t, 2, orderbook.OpenOrderCount())

	level := orderbook.PriceToBuyOrders[10.0]
	remaining := level.Snapshot()
	if assert.Len(t, remaining, 2) {
		assert.Equal(t, "buy-2", remaining[0].OrderID)
		assert.Equal(t, "buy-3", remaining[1].OrderID)
	}
	assert.Equal(t, 10.0, orderbook.BestBid.Peak())
}

func TestOrderbookCancelOrderRemovesMiddleAndPreservesFIFO(t *testing.T) {
	orderbook := NewOrderBook()
	now := time.Now().UTC().Unix()

	first := Order{OrderID: "sell-1", OrderType: "limit", Price: 11.0, Quantity: 2, Timestamp: now}
	second := Order{OrderID: "sell-2", OrderType: "limit", Price: 11.0, Quantity: 3, Timestamp: now}
	third := Order{OrderID: "sell-3", OrderType: "limit", Price: 11.0, Quantity: 4, Timestamp: now}

	orderbook.AddOrder(&first, "SELL")
	orderbook.AddOrder(&second, "SELL")
	orderbook.AddOrder(&third, "SELL")

	removed := orderbook.CancelOrder("sell-2")
	assert.True(t, removed)
	assert.Equal(t, 2, orderbook.OpenOrderCount())

	level := orderbook.PriceToSellOrders[11.0]
	remaining := level.Snapshot()
	if assert.Len(t, remaining, 2) {
		assert.Equal(t, "sell-1", remaining[0].OrderID)
		assert.Equal(t, "sell-3", remaining[1].OrderID)
	}
	assert.Equal(t, 11.0, orderbook.BestAsk.Peak())
}

func TestOrderbookCancelOrderRemovesLastLevelAndKeepsBestPriceInvariant(t *testing.T) {
	orderbook := NewOrderBook()
	now := time.Now().UTC().Unix()

	bestAsk := Order{OrderID: "ask-best", OrderType: "limit", Price: 10.0, Quantity: 2, Timestamp: now}
	nextAsk := Order{OrderID: "ask-next", OrderType: "limit", Price: 11.0, Quantity: 3, Timestamp: now}

	orderbook.AddOrder(&bestAsk, "SELL")
	orderbook.AddOrder(&nextAsk, "SELL")

	removed := orderbook.CancelOrder("ask-best")
	assert.True(t, removed)
	assert.Equal(t, 1, orderbook.OpenOrderCount())
	assert.Nil(t, orderbook.PriceToSellOrders[10.0])

	for orderbook.BestAsk.Len() > 0 {
		peak := orderbook.BestAsk.Peak().(float64)
		if _, ok := orderbook.PriceToSellOrders[peak]; ok {
			break
		}
		heap.Pop(orderbook.BestAsk)
	}

	assert.Equal(t, 11.0, orderbook.BestAsk.Peak())
}

func TestOrderbookCancelOrderMissingOrderIsNoOp(t *testing.T) {
	orderbook := NewOrderBook()
	now := time.Now().UTC().Unix()

	buy := Order{OrderID: "buy-1", OrderType: "limit", Price: 9.0, Quantity: 5, Timestamp: now}
	orderbook.AddOrder(&buy, "BUY")

	removed := orderbook.CancelOrder("unknown-order")
	assert.False(t, removed)
	assert.Equal(t, 1, orderbook.OpenOrderCount())

	level := orderbook.PriceToBuyOrders[9.0]
	remaining := level.Snapshot()
	if assert.Len(t, remaining, 1) {
		assert.Equal(t, "buy-1", remaining[0].OrderID)
	}
}
