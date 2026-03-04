package main

import (
	"container/heap"
	"context"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
)

type MatchingEngine struct {
	kafkaClient     *KafkaClient
	orderBook       *Orderbook
	metrics         *EngineMetrics
	quoteTopic      string
	tradeTopic      string
	pricePointTopic string
}

func NewMatchingEngine(kafkaClient *KafkaClient, orderBook *Orderbook) *MatchingEngine {
	me := new(MatchingEngine)
	me.kafkaClient = kafkaClient
	me.orderBook = orderBook
	me.metrics = nil
	me.quoteTopic = "orders.topic"
	me.tradeTopic = "trades.topic"
	me.pricePointTopic = "order.last_price.topic"
	return me
}

func (me *MatchingEngine) SetMetrics(metrics *EngineMetrics) {
	me.metrics = metrics
}

func (me *MatchingEngine) Start(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}

	orderMessages, errors := me.kafkaClient.ConsumerMessagesChan(me.quoteTopic)

	tradeProducer := me.kafkaClient.GetProducer()
	pricePointProducer := me.kafkaClient.GetProducer()

	var consumedCount atomic.Int64
	var producedCount atomic.Int64

	tradeChannel := make(chan Trade)
	go func(tradeMessages <-chan Trade) {
		logger := logWithMethod("kafka.produce.trade")
		for {
			select {
			case trade, ok := <-tradeMessages:
				if !ok {
					return
				}
				producerMessage := sarama.ProducerMessage{Topic: me.tradeTopic, Value: sarama.ByteEncoder(trade.toMessage())}
				par, off, err := (*tradeProducer).SendMessage(&producerMessage)
				if err != nil {
					logger.Error("trade produce failed", "topic", me.tradeTopic, "partition", par, "offset", off, "error", err, "order_id", trade.OrderId, "correlation_id", trade.TradeId)
				} else {
					logger.Info("trade produced", "topic", me.tradeTopic, "partition", par, "offset", off, "order_id", trade.OrderId, "correlation_id", trade.TradeId)
					producedCount.Add(1)
					if me.metrics != nil {
						me.metrics.ProducedMessagesCounter.Inc()
					}
				}
			case <-ctx.Done():
				logger.Info("context canceled, closing producer", "topic", me.tradeTopic)
				(*tradeProducer).Close()
				return
			}
		}
	}(tradeChannel)

	pricePointChannel := make(chan PricePoint)
	go func(pricePointMessages <-chan PricePoint) {
		logger := logWithMethod("kafka.produce.price_point")
		for {
			select {
			case pricePoint, ok := <-pricePointMessages:
				if !ok {
					return
				}
				producerMessage := sarama.ProducerMessage{Topic: me.pricePointTopic, Value: sarama.ByteEncoder(pricePoint.toMessage())}
				par, off, err := (*pricePointProducer).SendMessage(&producerMessage)
				if err != nil {
					logger.Error("price point produce failed", "topic", me.pricePointTopic, "partition", par, "offset", off, "error", err)
				} else {
					fmt.Println("INFO: produced price point:", producerMessage)
					producedCount.Add(1)
					if me.metrics != nil {
						me.metrics.ProducedMessagesCounter.Inc()
					}
				}
			case <-ctx.Done():
				logger.Info("context canceled, closing producer", "topic", me.pricePointTopic)
				(*pricePointProducer).Close()
				return
			}
		}
	}(pricePointChannel)
	go func() {
		logger := logWithMethod("matching.mid_price")
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				bestBid := 0.0
				if me.orderBook.BestBid.Len() > 0 {
					bestBid = me.orderBook.BestBid.Peak().(float64)
				}
				bestAsk := math.MaxFloat64
				if me.orderBook.BestAsk.Len() > 0 {
					bestAsk = me.orderBook.BestAsk.Peak().(float64)
				}
				if bestBid > 0 && bestAsk < math.MaxFloat64 {
					midPrice := (bestBid + bestAsk) / 2
					logger.Info("mid price emitted", "mid_price", midPrice)
					pricePointChannel <- createPricePoint(midPrice)
				}
			case <-ctx.Done():
				logger.Info("context canceled, stopping mid price publisher")
				return
			}
		}
	}()

	orderChannel := make(chan []byte)
	go func() {
		consumeLogger := logWithMethod("kafka.consume.order")
		for {
			select {
			case msg, ok := <-orderMessages:
				if !ok {
					consumeLogger.Info("consumer message channel closed", "topic", me.quoteTopic)
					orderChannel <- []byte{}
					return
				}
				if me.metrics != nil {
					me.metrics.ConsumedOrdersCounter.Inc()
				}
				order, err := messageToOrder(msg.Value)
				if err != nil {
					consumeLogger.Error("malformed order message", "topic", me.quoteTopic, "partition", msg.Partition, "offset", msg.Offset, "payload", string(msg.Value), "error", err)
					if me.metrics != nil {
						me.metrics.RejectedMalformedCounter.Inc()
					}
				} else {
					consumeLogger.Debug("order consumed", "topic", me.quoteTopic, "partition", msg.Partition, "offset", msg.Offset, "order_id", order.OrderID, "action", order.Action, "price", order.Price, "quantity", order.Quantity)
					consumedCount.Add(1)
				}
				me.Process(&order, tradeChannel, pricePointChannel)
			case consumerError, ok := <-errors:
				if !ok {
					consumeLogger.Info("consumer error channel closed", "topic", me.quoteTopic)
					orderChannel <- []byte{}
					return
				}
				consumedCount.Add(1)
				consumeLogger.Error("consumer error received", "topic", consumerError.Topic, "partition", consumerError.Partition, "error", consumerError.Err)
				orderChannel <- []byte{}
			case <-ctx.Done():
				consumeLogger.Info("context canceled, closing quote consumer", "topic", me.quoteTopic)
				orderChannel <- []byte{}
				return
			}
		}
	}()
	<-orderChannel
	logWithMethod("service.lifecycle").Info("matching engine stopped processing", "consumed_messages", consumedCount.Load(), "produced_messages", producedCount.Load())
}

func Min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func (me *MatchingEngine) Process(inOrder *Order, producerChannel chan<- Trade, pricePointChannel chan<- PricePoint) {
	processStart := time.Now()
	matchCount := 0
	defer func() {
		if me.metrics != nil {
			me.metrics.ProcessDurationHistogram.Observe(time.Since(processStart).Seconds())
			me.metrics.PerOrderMatchCountHistogram.Observe(float64(matchCount))
		}
		me.updateOrderbookGauges()
	}()

	var oppositeBook *map[float64]*OrderQueue
	var oppositeBestPrice Heap
	var inAction string
	var outAction string
	var comparator func(x, y float64) bool

	if strings.EqualFold(inOrder.Action, "BUY") {
		oppositeBook = &me.orderBook.PriceToSellOrders
		oppositeBestPrice = me.orderBook.BestAsk
		inAction = "BUY"
		outAction = "SELL"
		if inOrder.Quantity < 0 {
			inOrder.Quantity = -inOrder.Quantity
		}
		comparator = func(x, y float64) bool { return x >= y }
	} else if strings.EqualFold(inOrder.Action, "SELL") {
		oppositeBook = &me.orderBook.PriceToBuyOrders
		oppositeBestPrice = me.orderBook.BestBid
		inAction = "SELL"
		outAction = "BUY"
		if inOrder.Quantity < 0 {
			inOrder.Quantity = -inOrder.Quantity
		}
		comparator = func(x, y float64) bool { return x <= y }
	}

	// loop on opposite book
	for inOrder.Quantity > 0 && oppositeBestPrice.Len() > 0 && comparator(inOrder.Price, oppositeBestPrice.Peak().(float64)) {
		oppositeBestPriceQueue := (*oppositeBook)[oppositeBestPrice.Peak().(float64)]
		// loop on nest price queue
		for inOrder.Quantity > 0 && oppositeBestPriceQueue != nil && oppositeBestPriceQueue.Len() > 0 {
			outOrder := oppositeBestPriceQueue.PeekFront()
			if outOrder == nil {
				break
			}
			tradeQuantity := Min(inOrder.Quantity, outOrder.Quantity)
			price := outOrder.Price
			tradeId := uuid.New().String()
			ts := time.Now().Unix()
			matchCount++
			if me.metrics != nil {
				me.metrics.ExecutedTradesCounter.Inc()
			}
			logWithMethod("matching.process.trade").Info(
				"trade executed",
				"correlation_id", tradeId,
				"order_id", inOrder.OrderID,
				"counterparty_order_id", outOrder.OrderID,
				"action", inAction,
				"quantity", tradeQuantity,
				"price", price,
				"timestamp", ts,
				"remaining_in_order_quantity", inOrder.Quantity,
				"remaining_out_order_quantity", outOrder.Quantity,
			)

			inOrder.Quantity -= tradeQuantity
			outOrder.Quantity -= tradeQuantity

			if producerChannel != nil {
				producerChannel <- createTrade(tradeId, inOrder, tradeQuantity, price, inAction, ts)
				producerChannel <- createTrade(tradeId, outOrder, tradeQuantity, price, outAction, ts)
			}
			if pricePointChannel != nil {
				pricePointChannel <- createPricePoint(price)
			}

			if outOrder.Quantity == 0 {
				_, popped := oppositeBestPriceQueue.PopFront()
				if popped {
					me.orderBook.decrementOpenOrderCount()
				}
			}
		}
		if oppositeBestPriceQueue == nil || oppositeBestPriceQueue.Len() == 0 {
			bestPrice := heap.Pop(oppositeBestPrice).(float64)
			delete(*oppositeBook, bestPrice)
		}
	}
	if inOrder.Quantity > 0 { // don't add if empty
		me.orderBook.AddOrder(inOrder, inAction)
	}
}

func (me *MatchingEngine) updateOrderbookGauges() {
	if me.metrics == nil || me.orderBook == nil {
		return
	}

	bestBid := 0.0
	if me.orderBook.BestBid != nil && me.orderBook.BestBid.Len() > 0 {
		bestBid = me.orderBook.BestBid.Peak().(float64)
	}

	bestAsk := 0.0
	if me.orderBook.BestAsk != nil && me.orderBook.BestAsk.Len() > 0 {
		bestAsk = me.orderBook.BestAsk.Peak().(float64)
	}

	midPrice := 0.0
	spread := 0.0
	if bestBid > 0 && bestAsk > 0 {
		midPrice = (bestBid + bestAsk) / 2
		spread = bestAsk - bestBid
	}

	me.metrics.BestBidGauge.Set(bestBid)
	me.metrics.BestAskGauge.Set(bestAsk)
	me.metrics.MidPriceGauge.Set(midPrice)
	me.metrics.SpreadGauge.Set(spread)
	me.metrics.OpenOrderCountGauge.Set(float64(me.orderBook.OpenOrderCount()))
}
