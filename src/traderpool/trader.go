package main

import (
	"fmt"
	"math/rand"
	"time"
)

func GenerateAndPushOrder(trader Trader, orderChannel chan<- Order) {
	orderID := fmt.Sprintf("%s-%d", trader.TradeId, time.Now().UnixNano())
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Random quantity between 10 and 40 (always positive).
	quantity := uint64(10 + rng.Intn(30))
	action := "BUY"
	if rng.Float64() < 0.5 {
		action = "SELL"
	}

	priceAdjustment := rng.Float64() * 0.01
	var adjustedPrice float64
	if action == "BUY" {
		adjustedPrice = trader.Price*(1+priceAdjustment) + 0.5
	} else {
		adjustedPrice = trader.Price*(1-priceAdjustment) - 0.5
	}

	timestamp := time.Now().UnixNano()

	order := Order{
		OrderID:   orderID,
		OrderType: "limit",
		Price:     adjustedPrice,
		Quantity:  quantity,
		Action:    action,
		Timestamp: timestamp,
	}

	orderChannel <- order
	fmt.Printf("INFO: Trader %s generated order from price %.2f: %+v\n", trader.TradeId, trader.Price, order)
}
