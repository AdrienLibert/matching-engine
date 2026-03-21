package main

import "container/heap"

const (
	queueCompactMinHead = 64
)

type OrderQueue struct {
	items        []*Order
	head         int
	size         int
	orderIDToIdx map[string]int
}

func NewOrderQueue() *OrderQueue {
	// Complexity: O(1) time, O(1) additional space.
	return &OrderQueue{items: make([]*Order, 0), head: 0, size: 0, orderIDToIdx: make(map[string]int)}
}

func (q *OrderQueue) Push(order *Order) {
	// Complexity: amortized O(1) time for append; O(1) additional space unless growth triggers reallocation.
	if q == nil {
		return
	}
	insertAt := len(q.items)
	q.items = append(q.items, order)
	q.size++
	if order != nil && order.OrderID != "" {
		q.orderIDToIdx[order.OrderID] = insertAt
	}
}

func (q *OrderQueue) PeekFront() *Order {
	// Complexity: O(1) time, O(1) space.
	if q == nil || q.Len() == 0 {
		return nil
	}
	q.advanceHeadPastRemoved()
	if q.head >= len(q.items) {
		return nil
	}
	return q.items[q.head]
}

func (q *OrderQueue) PopFront() (*Order, bool) {
	// Complexity: amortized O(1) time; worst-case O(n) when compaction runs (n = active elements).
	// Space: O(1) normally; worst-case O(n) temporary allocation during compaction.
	if q == nil || q.Len() == 0 {
		return nil, false
	}
	q.advanceHeadPastRemoved()
	if q.head >= len(q.items) {
		q.compactIfNeeded()
		return nil, false
	}
	order := q.items[q.head]
	if order == nil {
		q.compactIfNeeded()
		return nil, false
	}
	q.items[q.head] = nil
	if order.OrderID != "" {
		delete(q.orderIDToIdx, order.OrderID)
	}
	q.head++
	q.size--
	q.compactIfNeeded()
	return order, true
}

func (q *OrderQueue) Remove(orderID string) bool {
	if q == nil || q.Len() == 0 || orderID == "" {
		return false
	}

	idx, ok := q.orderIDToIdx[orderID]
	if !ok || idx < q.head || idx >= len(q.items) {
		return false
	}

	order := q.items[idx]
	if order == nil {
		delete(q.orderIDToIdx, orderID)
		return false
	}

	q.items[idx] = nil
	delete(q.orderIDToIdx, orderID)
	q.size--

	if idx == q.head {
		q.advanceHeadPastRemoved()
	}
	q.compactIfNeeded()
	return true
}

func (q *OrderQueue) Snapshot() []*Order {
	// Test-only helper: currently used by unit tests to assert level contents.
	if q == nil || q.Len() == 0 {
		return []*Order{}
	}

	orders := make([]*Order, 0, q.Len())
	for index := q.head; index < len(q.items); index++ {
		if q.items[index] != nil {
			orders = append(orders, q.items[index])
		}
	}
	return orders
}

func (q *OrderQueue) Len() int {
	// Complexity: O(1) time, O(1) space.
	if q == nil {
		return 0
	}
	return q.size
}

func (q *OrderQueue) advanceHeadPastRemoved() {
	if q == nil {
		return
	}
	for q.head < len(q.items) && q.items[q.head] == nil {
		q.head++
	}
}

func (q *OrderQueue) compactIfNeeded() {
	// Complexity: O(1) when no compaction (or full-drain reset), O(n) when compacting (n = active elements copied).
	// Space: O(1) without compaction; O(n) additional space during compaction allocation.
	if q == nil {
		return
	}
	if q.size == 0 || q.head == len(q.items) {
		q.items = q.items[:0]
		q.head = 0
		q.size = 0
		q.orderIDToIdx = make(map[string]int)
		return
	}
	removedSlots := len(q.items) - q.size
	if q.head >= queueCompactMinHead && (q.head*2 >= len(q.items) || removedSlots >= queueCompactMinHead) {
		active := make([]*Order, 0, q.size)
		reindexed := make(map[string]int, q.size)
		for index := q.head; index < len(q.items); index++ {
			order := q.items[index]
			if order == nil {
				continue
			}
			active = append(active, order)
			if order.OrderID != "" {
				reindexed[order.OrderID] = len(active) - 1
			}
		}
		q.items = active
		q.head = 0
		q.orderIDToIdx = reindexed
	}
}

type Heap interface {
	heap.Interface
	Push(x interface{})
	Pop() interface{}
	Peak() interface{}
}

type MinHeap []float64

func (h MinHeap) Len() int { return len(h) }

func (h MinHeap) Less(i, j int) bool { return h[i] < h[j] }

func (h MinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *MinHeap) Push(x interface{}) { *h = append(*h, x.(float64)) }

func (h *MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *MinHeap) Peak() interface{} {
	n := len(*h)
	if n == 0 {
		return nil
	}
	return (*h)[0]
}

type MaxHeap []float64

func (h MaxHeap) Len() int { return len(h) }

func (h MaxHeap) Less(i, j int) bool { return h[i] > h[j] }

func (h MaxHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *MaxHeap) Push(x interface{}) { *h = append(*h, x.(float64)) }

func (h *MaxHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *MaxHeap) Peak() interface{} {
	n := len(*h)
	if n == 0 {
		return nil
	}
	return (*h)[0]
}

type Orderbook struct {
	BestBid        *MaxHeap
	BestAsk        *MinHeap
	PriceToVolume  map[float64]float64
	openOrderCount int
	// indexes + containers
	PriceToBuyOrders  map[float64]*OrderQueue
	PriceToSellOrders map[float64]*OrderQueue
	OrderIDToRef      map[string]OrderRef
}

type OrderRef struct {
	Action string
	Price  float64
}

func NewOrderBook() *Orderbook {
	o := new(Orderbook)
	o.BestBid = &MaxHeap{}
	o.BestAsk = &MinHeap{}
	o.PriceToBuyOrders = make(map[float64]*OrderQueue)
	o.PriceToSellOrders = make(map[float64]*OrderQueue)
	o.OrderIDToRef = make(map[string]OrderRef)
	return o
}

func (o *Orderbook) AddOrder(order *Order, orderAction string) {
	// Add order in orderbook
	// Rules:
	// - Hashmap of orders are indexes used to assess price in heaps exist
	// - Orders are added at the end of the list of orders
	price := order.Price

	if orderAction == "BUY" {
		val, ok := o.PriceToBuyOrders[price]
		if ok {
			val.Push(order)
		} else {
			heap.Push(o.BestBid, price)
			q := NewOrderQueue()
			q.Push(order)
			o.PriceToBuyOrders[price] = q
		}
		if order != nil && order.OrderID != "" {
			o.OrderIDToRef[order.OrderID] = OrderRef{Action: "BUY", Price: price}
		}
		o.openOrderCount++
	}
	if orderAction == "SELL" {
		val, ok := o.PriceToSellOrders[price]
		if ok {
			val.Push(order)
		} else {
			heap.Push(o.BestAsk, price)
			q := NewOrderQueue()
			q.Push(order)
			o.PriceToSellOrders[price] = q
		}
		if order != nil && order.OrderID != "" {
			o.OrderIDToRef[order.OrderID] = OrderRef{Action: "SELL", Price: price}
		}
		o.openOrderCount++
	}
}

func (o *Orderbook) unregisterOrder(orderID string) {
	// Remove Order ID from OrderIDToRef map
	if o == nil || orderID == "" {
		return
	}
	delete(o.OrderIDToRef, orderID)
}

func (o *Orderbook) CancelOrder(orderID string) bool {
	if o == nil || orderID == "" {
		return false
	}

	ref, ok := o.OrderIDToRef[orderID]
	if !ok {
		return false
	}

	// Fetch price level for order to cancel price
	var level *OrderQueue
	if ref.Action == "BUY" {
		level = o.PriceToBuyOrders[ref.Price]
	} else if ref.Action == "SELL" {
		level = o.PriceToSellOrders[ref.Price]
	}

	// TODO: in theory this is not possible, should we make it panic-free
	if level == nil {
		o.removePriceFromHeap(ref.Action, ref.Price)
		delete(o.OrderIDToRef, orderID)
		return false
	}

	// Flush order from level
	removed := level.Remove(orderID)
	if !removed {
		o.removePriceFromHeap(ref.Action, ref.Price)
		delete(o.OrderIDToRef, orderID)
		return false
	}

	o.decrementOpenOrderCount()
	delete(o.OrderIDToRef, orderID)

	// Flush level if empty, this is not good as new order might re alloc
	// price levels on demand. But we need to keep it for now to keep cache locality
	// we should consider offloading level management to a pool of price level malloc
	// which will go down the cache but still be faster than alloc
	if level.Len() == 0 {
		if ref.Action == "BUY" {
			delete(o.PriceToBuyOrders, ref.Price)
			o.removePriceFromHeap(ref.Action, ref.Price)
		}
		if ref.Action == "SELL" {
			delete(o.PriceToSellOrders, ref.Price)
			o.removePriceFromHeap(ref.Action, ref.Price)
		}
	}

	return true
}

func (o *Orderbook) decrementOpenOrderCount() {
	if o == nil {
		return
	}
	if o.openOrderCount > 0 {
		o.openOrderCount--
	}
}

func (o *Orderbook) OpenOrderCount() int {
	if o == nil {
		return 0
	}

	return o.openOrderCount
}

func (o *Orderbook) removePriceFromHeap(action string, price float64) {
	if o == nil {
		return
	}

	if action == "BUY" {
		o.removePriceFromMaxHeap(price)
		return
	}

	if action == "SELL" {
		o.removePriceFromMinHeap(price)
	}
}

func (o *Orderbook) removePriceFromMaxHeap(price float64) {
	if o == nil || o.BestBid == nil {
		return
	}

	for index, currentPrice := range *o.BestBid {
		if currentPrice == price {
			heap.Remove(o.BestBid, index)
			return
		}
	}
}

func (o *Orderbook) removePriceFromMinHeap(price float64) {
	if o == nil || o.BestAsk == nil {
		return
	}

	for index, currentPrice := range *o.BestAsk {
		if currentPrice == price {
			heap.Remove(o.BestAsk, index)
			return
		}
	}
}
