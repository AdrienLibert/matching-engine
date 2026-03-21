package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

const (
	orderChannelBufferSize      = 1000
	sourcePriceChannelBuffer    = 100
	perTraderPriceBufferSize    = 4
	periodicPriceBroadcastDelay = time.Second
)

type strategyFunc func(trader Trader, marketPrice float64, orderChannel chan<- Order)

type traderStrategy struct {
	name string
	run  strategyFunc
}

func getStrategyPool() []traderStrategy {
	return []traderStrategy{
		{
			name: "random",
			run: func(trader Trader, _ float64, orderChannel chan<- Order) {
				GenerateAndPushRandomOrder(trader, orderChannel)
			},
		},
		{
			name: "taker",
			run: func(trader Trader, _ float64, orderChannel chan<- Order) {
				GenerateMarketTakerOrder(trader, orderChannel)
			},
		},
		{
			name: "mean-reversion",
			run: func(trader Trader, marketPrice float64, orderChannel chan<- Order) {
				GenerateMeanReversionOrder(trader, marketPrice, orderChannel)
			},
		},
	}
}

func parseSimulationSeed() (int64, bool) {
	rawSeed := os.Getenv("SIM_SEED")
	if rawSeed == "" {
		return 0, false
	}

	parsedSeed, err := strconv.ParseInt(rawSeed, 10, 64)
	if err != nil {
		fmt.Printf("WARN: invalid SIM_SEED %q, using stochastic strategy selection\n", rawSeed)
		return 0, false
	}

	return parsedSeed, true
}

func newStrategySelectionRNG() *rand.Rand {
	if seed, ok := parseSimulationSeed(); ok {
		return rand.New(rand.NewSource(seed + 1))
	}

	return rand.New(rand.NewSource(time.Now().UnixNano()))
}

func selectTraderStrategy(rng *rand.Rand, strategies []traderStrategy) traderStrategy {
	if len(strategies) == 0 {
		panic("strategy pool must not be empty")
	}

	return strategies[rng.Intn(len(strategies))]
}

func assignStrategiesToTraders(numTraders int, rng *rand.Rand, strategies []traderStrategy) []traderStrategy {
	assignedStrategies := make([]traderStrategy, 0, numTraders)
	for i := 0; i < numTraders; i++ {
		assignedStrategies = append(assignedStrategies, selectTraderStrategy(rng, strategies))
	}

	return assignedStrategies
}

func Start(numTraders int, kc *KafkaClient) {
	orderProducer := kc.GetProducer()
	if orderProducer == nil {
		fmt.Println("ERROR: Kafka producer is nil! Exiting.")
		return
	}
	defer func() {
		if err := (*orderProducer).Close(); err != nil {
			fmt.Printf("ERROR: closing Kafka producer: %v\n", err)
		}
	}()

	master := kc.GetConsumer()
	if master == nil {
		fmt.Println("ERROR: Kafka consumer is nil! Exiting.")
		return
	}
	defer func() {
		if err := (*master).Close(); err != nil {
			fmt.Printf("ERROR: closing Kafka consumer: %v\n", err)
		}
	}()

	tradeConsumer, tradeErrors := kc.Assign(*master, kc.tradeTopic)
	priceConsumer, priceErrors := kc.Assign(*master, kc.pricePointTopic)

	orderChannel := make(chan Order, orderChannelBufferSize)
	sourcePriceUpdates := make(chan float64, sourcePriceChannelBuffer)

	done := make(chan struct{})

	var producerWG sync.WaitGroup
	producerWG.Add(1)

	go func() {
		defer producerWG.Done()
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

	traderPriceChannels := make([]chan float64, 0, numTraders)
	for i := 0; i < numTraders; i++ {
		traderPriceChannels = append(traderPriceChannels, make(chan float64, perTraderPriceBufferSize))
	}

	strategies := getStrategyPool()
	strategySelectionRNG := newStrategySelectionRNG()
	assignedStrategies := assignStrategiesToTraders(numTraders, strategySelectionRNG, strategies)

	var infraWG sync.WaitGroup
	infraWG.Add(2)

	go func() {
		defer infraWG.Done()
		consumePriceSources(
			done,
			tradeConsumer,
			tradeErrors,
			priceConsumer,
			priceErrors,
			sourcePriceUpdates,
			periodicPriceBroadcastDelay,
		)
	}()

	go func() {
		defer infraWG.Done()
		runPriceBroadcaster(sourcePriceUpdates, traderPriceChannels)
	}()

	var traderWG sync.WaitGroup
	for i := 0; i < numTraders; i++ {
		traderID := fmt.Sprintf("Trader-%d", i+1)
		traderPriceChannel := traderPriceChannels[i]
		assignedStrategy := assignedStrategies[i]
		fmt.Printf("INFO: assigned strategy=%s trader=%s\n", assignedStrategy.name, traderID)
		traderWG.Add(1)
		go func(id string, priceInput <-chan float64, strategy traderStrategy) {
			defer traderWG.Done()
			var referencePrice float64
			hasReferencePrice := false
			for price := range priceInput {
				if !hasReferencePrice {
					referencePrice = price
					hasReferencePrice = true
				}

				trader := Trader{TradeId: id, Price: referencePrice}
				strategy.run(trader, price, orderChannel)
				referencePrice = price
			}
		}(traderID, traderPriceChannel, assignedStrategy)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	defer signal.Stop(signals)
	<-signals

	close(done)
	infraWG.Wait()
	traderWG.Wait()
	close(orderChannel)
	producerWG.Wait()
}

func consumePriceSources(
	done <-chan struct{},
	tradeConsumer <-chan *sarama.ConsumerMessage,
	tradeErrors <-chan *sarama.ConsumerError,
	priceConsumer <-chan *sarama.ConsumerMessage,
	priceErrors <-chan *sarama.ConsumerError,
	priceUpdates chan<- float64,
	tickInterval time.Duration,
) {
	defer close(priceUpdates)

	var currentPrice float64
	hasObservedPrice := false
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case priceMsg, ok := <-priceConsumer:
			if !ok {
				priceConsumer = nil
				continue
			}

			pricePoint, err := convertMessageToPricePoint(priceMsg.Value)
			if err != nil {
				handleError(err)
				continue
			}

			currentPrice = pricePoint.Price
			hasObservedPrice = true
			if !publishPriceUpdate(done, priceUpdates, currentPrice) {
				return
			}
		case tradeMsg, ok := <-tradeConsumer:
			if !ok {
				tradeConsumer = nil
				continue
			}

			trade, err := convertMessageToTrade(tradeMsg.Value)
			if err != nil {
				handleError(err)
				continue
			}

			currentPrice = trade.Price
			hasObservedPrice = true
			if !publishPriceUpdate(done, priceUpdates, currentPrice) {
				return
			}
		case consumerError, ok := <-priceErrors:
			if !ok {
				priceErrors = nil
				continue
			}
			if consumerError != nil && consumerError.Err != nil {
				handleError(consumerError.Err)
			}
		case consumerError, ok := <-tradeErrors:
			if !ok {
				tradeErrors = nil
				continue
			}
			if consumerError != nil && consumerError.Err != nil {
				handleError(consumerError.Err)
			}
		case <-ticker.C:
			if !hasObservedPrice {
				continue
			}
			if !publishPriceUpdate(done, priceUpdates, currentPrice) {
				return
			}
		}
	}
}

func runPriceBroadcaster(priceUpdates <-chan float64, traderPriceChannels []chan float64) {
	defer func() {
		for _, traderPriceChannel := range traderPriceChannels {
			close(traderPriceChannel)
		}
	}()

	for price := range priceUpdates {
		fanOutPriceUpdate(price, traderPriceChannels)
	}
}

func fanOutPriceUpdate(price float64, traderPriceChannels []chan float64) {
	for _, traderPriceChannel := range traderPriceChannels {
		select {
		case traderPriceChannel <- price:
		default:
		}
	}
}

func publishPriceUpdate(done <-chan struct{}, priceUpdates chan<- float64, price float64) bool {
	select {
	case <-done:
		return false
	case priceUpdates <- price:
		return true
	}
}
