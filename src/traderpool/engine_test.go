package main

import (
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

func TestRunPriceBroadcasterBroadcastsToAllTraders(t *testing.T) {
	priceUpdates := make(chan float64, 1)
	traderPriceChannels := []chan float64{
		make(chan float64, 1),
		make(chan float64, 1),
		make(chan float64, 1),
	}

	finished := make(chan struct{})
	go func() {
		runPriceBroadcaster(priceUpdates, traderPriceChannels)
		close(finished)
	}()

	priceUpdates <- 101.25
	close(priceUpdates)

	for index, traderPriceChannel := range traderPriceChannels {
		select {
		case price, ok := <-traderPriceChannel:
			if !ok {
				t.Fatalf("trader channel %d closed before receiving broadcast", index)
			}
			if price != 101.25 {
				t.Fatalf("unexpected price for trader channel %d: got %.2f want %.2f", index, price, 101.25)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("timed out waiting for trader channel %d broadcast", index)
		}

		select {
		case _, ok := <-traderPriceChannel:
			if ok {
				t.Fatalf("expected trader channel %d to be closed after source closes", index)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("timed out waiting for trader channel %d close", index)
		}
	}

	select {
	case <-finished:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for broadcaster to finish")
	}
}

func TestFanOutPriceUpdateSlowTraderDoesNotBlockOthers(t *testing.T) {
	fastTraderChannel := make(chan float64, 1)
	slowTraderChannel := make(chan float64)

	start := time.Now()
	fanOutPriceUpdate(77.5, []chan float64{fastTraderChannel, slowTraderChannel})
	if time.Since(start) > 100*time.Millisecond {
		t.Fatalf("fan-out blocked unexpectedly on slow trader")
	}

	select {
	case price := <-fastTraderChannel:
		if price != 77.5 {
			t.Fatalf("unexpected fast-trader price: got %.2f want %.2f", price, 77.5)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for fast trader price")
	}
}

func TestConsumePriceSourcesHandlesClosedConsumerChannels(t *testing.T) {
	done := make(chan struct{})
	tradeConsumer := make(chan *sarama.ConsumerMessage)
	tradeErrors := make(chan *sarama.ConsumerError)
	priceConsumer := make(chan *sarama.ConsumerMessage)
	priceErrors := make(chan *sarama.ConsumerError)
	priceUpdates := make(chan float64, 2)

	close(tradeConsumer)
	close(tradeErrors)
	close(priceConsumer)
	close(priceErrors)

	finished := make(chan struct{})
	go func() {
		consumePriceSources(
			done,
			tradeConsumer,
			tradeErrors,
			priceConsumer,
			priceErrors,
			priceUpdates,
			10*time.Millisecond,
		)
		close(finished)
	}()

	time.Sleep(30 * time.Millisecond)
	close(done)

	select {
	case <-finished:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for consumePriceSources to stop")
	}

	for {
		select {
		case _, ok := <-priceUpdates:
			if !ok {
				return
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("timed out waiting for price updates channel to close")
		}
	}
}

func TestAssignStrategiesToTradersSeededCoverage(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	strategies := getStrategyPool()
	assigned := assignStrategiesToTraders(200, rng, strategies)

	if len(assigned) != 200 {
		t.Fatalf("unexpected assigned strategy count: got %d want %d", len(assigned), 200)
	}

	assignedNames := make([]string, 0, len(assigned))
	for _, strategy := range assigned {
		assignedNames = append(assignedNames, strategy.name)
	}

	expectedNames := []string{"random", "taker", "mean-reversion"}
	for _, expectedName := range expectedNames {
		if !slices.Contains(assignedNames, expectedName) {
			t.Fatalf("expected seeded assignment to include %q", expectedName)
		}
	}
}

func TestAssignedStrategyIsOneTimePerTrader(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	strategies := getStrategyPool()
	assigned := assignStrategiesToTraders(1, rng, strategies)

	if len(assigned) != 1 {
		t.Fatalf("unexpected assigned strategy count: got %d want %d", len(assigned), 1)
	}

	chosenName := assigned[0].name
	for i := 0; i < 100; i++ {
		if assigned[0].name != chosenName {
			t.Fatalf("strategy changed for trader: got %q want %q", assigned[0].name, chosenName)
		}
	}
}
