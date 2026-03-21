package main

import (
	"fmt"
	"math"
	"math/rand"
	"time"
)

type Trader struct {
	TradeId string  `json:"trade_id"`
	Price   float64 `json:"price"`
}

func GenerateAndPushRandomOrder(trader Trader, orderChannel chan<- Order) {
	// Re-seeding every call is expensive; ideally, pass a shared RNG or use crypto/rand
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// 1. Dynamic Quantity (Log-Normal makes for more realistic "whale" orders)
	// Most orders are small (~10-20), but some are very large.
	quantity := uint64(10 + math.Exp(rng.NormFloat64()*0.5)*10)

	// 2. Determine Action
	action := "BUY"
	if rng.Float64() < 0.5 {
		action = "SELL"
	}

	// 3. Volatility via Normal Distribution
	// rng.NormFloat64() returns values centered at 0 with a std dev of 1.
	// volatilityScale controls how wide the order book depth becomes.
	volatilityScale := 0.02 // 2% standard deviation
	priceShift := rng.NormFloat64() * volatilityScale

	// We use Abs() because the "Side" already determines the direction.
	// We want the noise to push orders AWAY from the midprice to build depth.
	offset := math.Abs(priceShift)

	var adjustedPrice float64
	if action == "BUY" {
		// Buy orders placed BELOW midprice (liquidity providing)
		adjustedPrice = trader.Price * (1.0 - offset)
	} else {
		// Sell orders placed ABOVE midprice
		adjustedPrice = trader.Price * (1.0 + offset)
	}

	// 4. Ensure we don't have negative prices or zero quantities
	if adjustedPrice <= 0 {
		adjustedPrice = trader.Price
	}

	order := Order{
		OrderID:   fmt.Sprintf("RANDOM-%d", time.Now().UnixNano()),
		OrderType: "limit",
		Price:     adjustedPrice,
		Quantity:  quantity,
		Action:    action,
		Timestamp: time.Now().UnixNano(),
	}

	orderChannel <- order
}

func GenerateMarketTakerOrder(trader Trader, orderChannel chan<- Order) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Corrected: Use Int63n for int64 or Intn with a cast
	quantity := uint64(5 + rng.Intn(15))

	action := "BUY"
	if rng.Float64() < 0.5 {
		action = "SELL"
	}

	// This trader "crosses the spread" to ensure an immediate match
	// Higher aggression = higher volatility
	aggression := 0.01 // 1% push past the midprice

	var executionPrice float64
	if action == "BUY" {
		executionPrice = trader.Price * (1 + aggression)
	} else {
		executionPrice = trader.Price * (1 - aggression)
	}

	orderChannel <- Order{
		OrderID:   fmt.Sprintf("TAKER-%d", time.Now().UnixNano()),
		OrderType: "limit",
		Price:     executionPrice,
		Quantity:  quantity,
		Action:    action,
		Timestamp: time.Now().UnixNano(),
	}
}

func GenerateMeanReversionOrder(trader Trader, lobMidPrice float64, orderChannel chan<- Order) {
	// If LOB is too high, sell. If LOB is too low, buy.
	diff := (lobMidPrice - trader.Price) / trader.Price

	// Threshold: Only act if price is > 1% away from "fair value"
	if math.Abs(diff) < 0.01 {
		return
	}

	action := "SELL"
	if diff < 0 {
		action = "BUY"
	}

	// Place order exactly at the "Fair Price" to pull the market back
	orderChannel <- Order{
		OrderID:   fmt.Sprintf("ARBITRAGE-%d", time.Now().UnixNano()),
		OrderType: "limit",
		Price:     trader.Price,
		Quantity:  20, // Solid block of liquidity
		Action:    action,
	}
}
