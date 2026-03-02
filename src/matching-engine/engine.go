package main

import (
	"container/heap"
	"fmt"
	"math"
	"os"
	"os/signal"
	"strings"
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

func (me *MatchingEngine) Start() {
	orderMessages, errors := me.kafkaClient.ConsumerMessagesChan(me.quoteTopic)

	tradeProducer := me.kafkaClient.GetProducer()
	pricePointProducer := me.kafkaClient.GetProducer()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	consumedCount := 0
	producedCount := 0

	tradeChannel := make(chan Trade)
	go func(tradeMessages <-chan Trade) {
		for {
			select {
			case trade := <-tradeMessages:
				producerMessage := sarama.ProducerMessage{Topic: me.tradeTopic, Value: sarama.ByteEncoder(trade.toMessage())}
				par, off, err := (*tradeProducer).SendMessage(&producerMessage)
				if err != nil {
					fmt.Printf("ERROR: producing trade in partition %d, offset %d: %s", par, off, err)
				} else {
					fmt.Printf(
						"INFO: produced trade topic=%s partition=%d offset=%d trade_id=%s order_id=%s action=%s quantity=%d price=%.6f status=%s timestamp=%d\n",
						me.tradeTopic,
						par,
						off,
						trade.TradeId,
						trade.OrderId,
						trade.Action,
						trade.Quantity,
						trade.Price,
						trade.Status,
						trade.Timestamp,
					)
					producedCount++
					if me.metrics != nil {
						me.metrics.ProducedMessagesCounter.Inc()
					}
				}
			case <-signals:
				fmt.Println("INFO: interrupt is detected... closing trade producer...")
				(*tradeProducer).Close()
				tradeChannel <- Trade{}
			}
		}
	}(tradeChannel)

	pricePointChannel := make(chan PricePoint)
	go func(pricePointMessages <-chan PricePoint) {
		for {
			select {
			case pricePoint := <-pricePointMessages:
				producerMessage := sarama.ProducerMessage{Topic: me.pricePointTopic, Value: sarama.ByteEncoder(pricePoint.toMessage())}
				par, off, err := (*pricePointProducer).SendMessage(&producerMessage)
				if err != nil {
					fmt.Printf("ERROR: producing price point in partition %d, offset %d: %s", par, off, err)
				} else {
					fmt.Printf(
						"INFO: produced price point topic=%s partition=%d offset=%d price=%.6f\n",
						me.pricePointTopic,
						par,
						off,
						pricePoint.Price,
					)
					producedCount++
					if me.metrics != nil {
						me.metrics.ProducedMessagesCounter.Inc()
					}
				}
			case <-signals:
				fmt.Println("INFO: interrupt is detected... closing price point producer...")
				(*pricePointProducer).Close()
				pricePointChannel <- PricePoint{}
			}
		}
	}(pricePointChannel)
	go func() {
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
					fmt.Printf("INFO: produced midprice every 1s: %.2f\n", midPrice)
					pricePointChannel <- createPricePoint(midPrice)
				}
			case <-signals:
				fmt.Println("INFO: interrupt is detected... Closing quote midprice...")
				return
			}
		}
	}()

	orderChannel := make(chan []byte)
	go func() {
		for {
			select {
			case msg := <-orderMessages:
				if me.metrics != nil {
					me.metrics.ConsumedOrdersCounter.Inc()
				}
				order, err := messageToOrder(msg.Value)
				if err != nil {
					fmt.Println("ERROR: malformed message:", msg.Value)
					if me.metrics != nil {
						me.metrics.RejectedMalformedCounter.Inc()
					}
				} else {
					fmt.Println("DEBUG: received quote:", order)
					consumedCount++
				}
				me.Process(&order, tradeChannel, pricePointChannel)
			case consumerError := <-errors:
				consumedCount++
				fmt.Println("ERROR: received consumerError:", string(consumerError.Topic), string(consumerError.Partition), consumerError.Err)
				orderChannel <- []byte{}
			case <-signals:
				fmt.Println("INFO: interrupt is detected... Closing quote consummer...")
				orderChannel <- []byte{}
			}
		}
	}()
	<-orderChannel
	fmt.Println("INFO: closing... processed", consumedCount, "messages and produced", producedCount, "messages")
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

	var oppositeBook *map[float64]*[]*Order
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
	} else if inOrder.Quantity > 0 {
		oppositeBook = &me.orderBook.PriceToSellOrders
		oppositeBestPrice = me.orderBook.BestAsk
		inAction = "BUY"
		outAction = "SELL"
		comparator = func(x, y float64) bool { return x >= y }
	} else {
		oppositeBook = &me.orderBook.PriceToBuyOrders
		oppositeBestPrice = me.orderBook.BestBid
		inAction = "SELL"
		outAction = "BUY"
		inOrder.Quantity = -inOrder.Quantity
		comparator = func(x, y float64) bool { return x <= y }
	}

	// loop on opposite book
	for inOrder.Quantity > 0 && oppositeBestPrice.Len() > 0 && comparator(inOrder.Price, oppositeBestPrice.Peak().(float64)) {
		oppositeBestPriceQueue := (*oppositeBook)[oppositeBestPrice.Peak().(float64)]
		// loop on nest price queue
		for inOrder.Quantity > 0 && len(*oppositeBestPriceQueue) > 0 {
			outOrder, _ := cut(0, oppositeBestPriceQueue)
			tradeQuantity := Min(inOrder.Quantity, outOrder.Quantity)
			price := outOrder.Price
			tradeId := uuid.New().String()
			ts := time.Now().Unix()
			matchCount++
			if me.metrics != nil {
				me.metrics.ExecutedTradesCounter.Inc()
			}
			fmt.Printf(
				"INFO: Executed trade %s @ %d: %s %d @ %f | Left Order ID: %s, Right Order ID: %s | "+
					"Left Quantity: %d, Right Quantity: %d\n",
				tradeId, ts, inAction, tradeQuantity, price, inOrder.OrderID, outOrder.OrderID,
				inOrder.Quantity, outOrder.Quantity,
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

			if outOrder.Quantity > 0 {
				me.orderBook.AddOrder(outOrder, outAction)
			}
		}
		if len(*oppositeBestPriceQueue) == 0 {
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
	me.metrics.OpenOrderCountGauge.Set(float64(me.openOrderCount()))
}

func (me *MatchingEngine) openOrderCount() int {
	if me.orderBook == nil {
		return 0
	}

	count := 0
	for _, orders := range me.orderBook.PriceToBuyOrders {
		if orders != nil {
			count += len(*orders)
		}
	}
	for _, orders := range me.orderBook.PriceToSellOrders {
		if orders != nil {
			count += len(*orders)
		}
	}

	return count
}
