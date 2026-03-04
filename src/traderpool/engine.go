package main

import (
	"fmt"
	"github.com/IBM/sarama"
	"os"
	"os/signal"
	"time"
)

func Start(numTraders int, kc *KafkaClient) {
	orderProducer := kc.GetProducer()
	if orderProducer == nil {
		fmt.Println("ERROR: Kafka producer is nil! Exiting.")
		return
	}

	master := kc.GetConsumer()
	tradeConsumer, _ := kc.Assign(*master, kc.tradeTopic)
	priceConsumer, _ := kc.Assign(*master, kc.pricePointTopic)

	// Channel for orders, with a larger buffer for 6,000 traders
	orderChannel := make(chan Order, 1000)

	// Goroutine to produce orders to Kafka
	go func() {
		for order := range orderChannel {
			if order.Quantity == 0 {
				continue
			}
			producerMessage := sarama.ProducerMessage{
				Topic: kc.quoteTopic,
				Value: sarama.ByteEncoder(convertOrderToMessage(order)),
			}
			_, _, err := (*orderProducer).SendMessage(&producerMessage)
			if err != nil {
				fmt.Printf("ERROR: producing : %s\n", err)
			} else {
				fmt.Println("INFO: produced :", order)
			}
		}
	}()

	// Channel to broadcast price updates to all traders
	priceUpdateChannel := make(chan float64, 100)

	// Goroutine to consume price points and broadcast the current price
	go func() {
		currentPrice := 50.0 // Starting price
		for {
			select {
			case priceMsg := <-priceConsumer:
				pricePoint, err := convertMessageToPricePoint(priceMsg.Value)
				if err == nil {
					currentPrice = pricePoint.Price
					priceUpdateChannel <- currentPrice
				}
			case tradeMsg := <-tradeConsumer:
				trade, err := convertMessageToTrade(tradeMsg.Value)
				if err == nil {
					currentPrice = trade.Price // Update price from trades
					priceUpdateChannel <- currentPrice
				}
			case <-time.After(1 * time.Second):
				// Send the current price every second, even without updates
				priceUpdateChannel <- currentPrice
			}
		}
	}()

	// Start all traders
	for i := 0; i < numTraders; i++ {
		traderID := fmt.Sprintf("Trader-%d", i+1)
		go func(id string) {
			for price := range priceUpdateChannel {
				trader := Trader{TradeId: id, Price: price}
				GenerateAndPushOrder(trader, orderChannel)
			}
		}(traderID)
	}

	// Handle shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	<-signals
}
