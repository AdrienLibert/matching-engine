package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func collectTrades(tradeChannel <-chan Trade, count int) []Trade {
	trades := make([]Trade, 0, count)
	for index := 0; index < count; index++ {
		trades = append(trades, <-tradeChannel)
	}
	return trades
}

func collectPricePoints(pricePointChannel <-chan PricePoint, count int) []PricePoint {
	pricePoints := make([]PricePoint, 0, count)
	for index := 0; index < count; index++ {
		pricePoints = append(pricePoints, <-pricePointChannel)
	}
	return pricePoints
}

func levelQuantities(level *OrderQueue) []int64 {
	if level == nil {
		return []int64{}
	}

	quantities := make([]int64, 0, level.Len())
	for _, order := range level.items[level.head:] {
		quantities = append(quantities, order.Quantity)
	}

	return quantities
}

func levelOrderIDs(level *OrderQueue) []string {
	if level == nil {
		return []string{}
	}

	orderIDs := make([]string, 0, level.Len())
	for _, order := range level.items[level.head:] {
		orderIDs = append(orderIDs, order.OrderID)
	}

	return orderIDs
}

func TestMatchingEngineProcessScenarios(t *testing.T) {
	now := time.Now().UTC().Unix()

	type scenario struct {
		name     string
		setup    []Order
		incoming Order
		assertFn func(t *testing.T, orderbook *Orderbook)
	}

	testCases := []scenario{
		{
			name: "no-match",
			setup: []Order{
				{OrderID: "resting-sell-1", OrderType: "limit", Price: 12.0, Quantity: 5, Action: "SELL", Timestamp: now},
				{OrderID: "resting-sell-2", OrderType: "limit", Price: 12.0, Quantity: 3, Action: "SELL", Timestamp: now},
			},
			incoming: Order{OrderID: "incoming-buy", OrderType: "limit", Price: 11.0, Quantity: 5, Action: "BUY", Timestamp: now},
			assertFn: func(t *testing.T, orderbook *Orderbook) {
				assert.Equal(t, 1, orderbook.BestBid.Len())
				assert.Equal(t, 11.0, orderbook.BestBid.Peak())
				assert.Equal(t, []int64{5}, levelQuantities(orderbook.PriceToBuyOrders[11.0]))
				assert.Equal(t, []string{"incoming-buy"}, levelOrderIDs(orderbook.PriceToBuyOrders[11.0]))

				assert.Equal(t, 1, orderbook.BestAsk.Len())
				assert.Equal(t, 12.0, orderbook.BestAsk.Peak())
				assert.Equal(t, []int64{5, 3}, levelQuantities(orderbook.PriceToSellOrders[12.0]))
				assert.Equal(t, []string{"resting-sell-1", "resting-sell-2"}, levelOrderIDs(orderbook.PriceToSellOrders[12.0]))
			},
		},
		{
			name: "full-fill",
			setup: []Order{
				{OrderID: "resting-sell", OrderType: "limit", Price: 11.0, Quantity: 7, Action: "SELL", Timestamp: now},
			},
			incoming: Order{OrderID: "incoming-buy", OrderType: "limit", Price: 11.0, Quantity: 7, Action: "BUY", Timestamp: now},
			assertFn: func(t *testing.T, orderbook *Orderbook) {
				assert.Equal(t, 0, orderbook.BestBid.Len())
				assert.Equal(t, nil, orderbook.BestBid.Peak())
				assert.Equal(t, 0, len(orderbook.PriceToBuyOrders))

				assert.Equal(t, 0, orderbook.BestAsk.Len())
				assert.Equal(t, nil, orderbook.BestAsk.Peak())
				assert.Equal(t, 0, len(orderbook.PriceToSellOrders))
			},
		},
		{
			name: "partial-fill",
			setup: []Order{
				{OrderID: "resting-sell-1", OrderType: "limit", Price: 11.0, Quantity: 10, Action: "SELL", Timestamp: now},
				{OrderID: "resting-sell-2", OrderType: "limit", Price: 11.0, Quantity: 2, Action: "SELL", Timestamp: now},
			},
			incoming: Order{OrderID: "incoming-buy", OrderType: "limit", Price: 11.0, Quantity: 4, Action: "BUY", Timestamp: now},
			assertFn: func(t *testing.T, orderbook *Orderbook) {
				assert.Equal(t, 0, orderbook.BestBid.Len())
				assert.Equal(t, nil, orderbook.BestBid.Peak())
				assert.Equal(t, 0, len(orderbook.PriceToBuyOrders))

				assert.Equal(t, 1, orderbook.BestAsk.Len())
				assert.Equal(t, 11.0, orderbook.BestAsk.Peak())
				assert.Equal(t, []int64{6, 2}, levelQuantities(orderbook.PriceToSellOrders[11.0]))
				assert.Equal(t, []string{"resting-sell-1", "resting-sell-2"}, levelOrderIDs(orderbook.PriceToSellOrders[11.0]))
			},
		},
		{
			name: "multi-level-sweep",
			setup: []Order{
				{OrderID: "sell-l1a", OrderType: "limit", Price: 10.0, Quantity: 3, Action: "SELL", Timestamp: now},
				{OrderID: "sell-l1b", OrderType: "limit", Price: 10.0, Quantity: 2, Action: "SELL", Timestamp: now},
				{OrderID: "sell-l2", OrderType: "limit", Price: 11.0, Quantity: 4, Action: "SELL", Timestamp: now},
				{OrderID: "sell-l3", OrderType: "limit", Price: 12.0, Quantity: 8, Action: "SELL", Timestamp: now},
			},
			incoming: Order{OrderID: "incoming-buy", OrderType: "limit", Price: 11.0, Quantity: 10, Action: "BUY", Timestamp: now},
			assertFn: func(t *testing.T, orderbook *Orderbook) {
				assert.Equal(t, 1, orderbook.BestBid.Len())
				assert.Equal(t, 11.0, orderbook.BestBid.Peak())
				assert.Equal(t, []int64{1}, levelQuantities(orderbook.PriceToBuyOrders[11.0]))
				assert.Equal(t, []string{"incoming-buy"}, levelOrderIDs(orderbook.PriceToBuyOrders[11.0]))

				assert.Equal(t, 1, orderbook.BestAsk.Len())
				assert.Equal(t, 12.0, orderbook.BestAsk.Peak())
				assert.Equal(t, []int64{8}, levelQuantities(orderbook.PriceToSellOrders[12.0]))
				assert.Equal(t, []string{"sell-l3"}, levelOrderIDs(orderbook.PriceToSellOrders[12.0]))
				assert.Equal(t, 0, len(levelOrderIDs(orderbook.PriceToSellOrders[10.0])))
				assert.Equal(t, 0, len(levelOrderIDs(orderbook.PriceToSellOrders[11.0])))
			},
		},
	}

	for _, testCase := range testCases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			orderbook := NewOrderBook()
			matchingEngine := NewMatchingEngine(nil, orderbook)

			for index := range testCase.setup {
				setupOrder := testCase.setup[index]
				matchingEngine.Process(&setupOrder, nil, nil)
			}

			incomingOrder := testCase.incoming
			matchingEngine.Process(&incomingOrder, nil, nil)

			if testCase.assertFn == nil {
				t.Fatalf("missing assertions for scenario %s", testCase.name)
			}
			testCase.assertFn(t, orderbook)
		})
	}

	if len(testCases) != 4 {
		t.Fatalf("unexpected scenario count: got %d, want 4", len(testCases))
	}

	scenarioNames := make([]string, 0, len(testCases))
	for _, testCase := range testCases {
		scenarioNames = append(scenarioNames, testCase.name)
	}
	assert.Equal(t, []string{"no-match", "full-fill", "partial-fill", "multi-level-sweep"}, scenarioNames, fmt.Sprintf("unexpected scenario set: %v", scenarioNames))
}

func TestMatchingEngineProcessFIFOAndTradeOrderBuyAgainstSamePriceSells(t *testing.T) {
	for run := 0; run < 5; run++ {
		orderbook := NewOrderBook()
		matchingEngine := NewMatchingEngine(nil, orderbook)
		now := time.Now().UTC().Unix()

		restingSell1 := Order{OrderID: "sell-1", OrderType: "limit", Price: 10.0, Quantity: 2, Action: "SELL", Timestamp: now}
		restingSell2 := Order{OrderID: "sell-2", OrderType: "limit", Price: 10.0, Quantity: 3, Action: "SELL", Timestamp: now}
		matchingEngine.Process(&restingSell1, nil, nil)
		matchingEngine.Process(&restingSell2, nil, nil)

		tradeChannel := make(chan Trade, 8)
		incomingBuy := Order{OrderID: "buy-in", OrderType: "limit", Price: 10.0, Quantity: 5, Action: "BUY", Timestamp: now}
		matchingEngine.Process(&incomingBuy, tradeChannel, nil)

		trades := collectTrades(tradeChannel, 4)

		assert.Equal(t, []string{"buy-in", "sell-1", "buy-in", "sell-2"}, []string{trades[0].OrderId, trades[1].OrderId, trades[2].OrderId, trades[3].OrderId})
		assert.Equal(t, []string{"BUY", "SELL", "BUY", "SELL"}, []string{trades[0].Action, trades[1].Action, trades[2].Action, trades[3].Action})
		assert.Equal(t, []int64{2, 2, 3, 3}, []int64{trades[0].Quantity, trades[1].Quantity, trades[2].Quantity, trades[3].Quantity})
		assert.Equal(t, []string{"partial", "closed", "closed", "closed"}, []string{trades[0].Status, trades[1].Status, trades[2].Status, trades[3].Status})

		for _, trade := range trades {
			assert.Equal(t, 10.0, trade.Price)
		}

		assert.Equal(t, 0, orderbook.BestBid.Len())
		assert.Equal(t, 0, orderbook.BestAsk.Len())
		assert.Equal(t, 0, len(orderbook.PriceToBuyOrders))
		assert.Equal(t, 0, len(orderbook.PriceToSellOrders))
	}
}

func TestMatchingEngineProcessFIFOAndTradeOrderSellAgainstSamePriceBuys(t *testing.T) {
	for run := 0; run < 5; run++ {
		orderbook := NewOrderBook()
		matchingEngine := NewMatchingEngine(nil, orderbook)
		now := time.Now().UTC().Unix()

		restingBuy1 := Order{OrderID: "buy-1", OrderType: "limit", Price: 10.0, Quantity: 2, Action: "BUY", Timestamp: now}
		restingBuy2 := Order{OrderID: "buy-2", OrderType: "limit", Price: 10.0, Quantity: 3, Action: "BUY", Timestamp: now}
		matchingEngine.Process(&restingBuy1, nil, nil)
		matchingEngine.Process(&restingBuy2, nil, nil)

		tradeChannel := make(chan Trade, 8)
		incomingSell := Order{OrderID: "sell-in", OrderType: "limit", Price: 10.0, Quantity: 5, Action: "SELL", Timestamp: now}
		matchingEngine.Process(&incomingSell, tradeChannel, nil)

		trades := collectTrades(tradeChannel, 4)

		assert.Equal(t, []string{"sell-in", "buy-1", "sell-in", "buy-2"}, []string{trades[0].OrderId, trades[1].OrderId, trades[2].OrderId, trades[3].OrderId})
		assert.Equal(t, []string{"SELL", "BUY", "SELL", "BUY"}, []string{trades[0].Action, trades[1].Action, trades[2].Action, trades[3].Action})
		assert.Equal(t, []int64{2, 2, 3, 3}, []int64{trades[0].Quantity, trades[1].Quantity, trades[2].Quantity, trades[3].Quantity})
		assert.Equal(t, []string{"partial", "closed", "closed", "closed"}, []string{trades[0].Status, trades[1].Status, trades[2].Status, trades[3].Status})

		for _, trade := range trades {
			assert.Equal(t, 10.0, trade.Price)
		}

		assert.Equal(t, 0, orderbook.BestBid.Len())
		assert.Equal(t, 0, orderbook.BestAsk.Len())
		assert.Equal(t, 0, len(orderbook.PriceToBuyOrders))
		assert.Equal(t, 0, len(orderbook.PriceToSellOrders))
	}
}

func TestMatchingEngineProcessEventFieldsBuyFlow(t *testing.T) {
	orderbook := NewOrderBook()
	matchingEngine := NewMatchingEngine(nil, orderbook)
	now := time.Now().UTC().Unix()

	restingSell1 := Order{OrderID: "sell-1", OrderType: "limit", Price: 10.0, Quantity: 2, Action: "SELL", Timestamp: now}
	restingSell2 := Order{OrderID: "sell-2", OrderType: "limit", Price: 10.0, Quantity: 3, Action: "SELL", Timestamp: now}
	restingSell3 := Order{OrderID: "sell-3", OrderType: "limit", Price: 11.0, Quantity: 4, Action: "SELL", Timestamp: now}
	matchingEngine.Process(&restingSell1, nil, nil)
	matchingEngine.Process(&restingSell2, nil, nil)
	matchingEngine.Process(&restingSell3, nil, nil)

	tradeChannel := make(chan Trade, 12)
	pricePointChannel := make(chan PricePoint, 12)
	incomingBuy := Order{OrderID: "buy-in", OrderType: "limit", Price: 11.0, Quantity: 7, Action: "BUY", Timestamp: now}
	matchingEngine.Process(&incomingBuy, tradeChannel, pricePointChannel)

	trades := collectTrades(tradeChannel, 6)
	pricePoints := collectPricePoints(pricePointChannel, 3)

	assert.Equal(t, []string{"buy-in", "sell-1", "buy-in", "sell-2", "buy-in", "sell-3"}, []string{trades[0].OrderId, trades[1].OrderId, trades[2].OrderId, trades[3].OrderId, trades[4].OrderId, trades[5].OrderId})
	assert.Equal(t, []string{"BUY", "SELL", "BUY", "SELL", "BUY", "SELL"}, []string{trades[0].Action, trades[1].Action, trades[2].Action, trades[3].Action, trades[4].Action, trades[5].Action})
	assert.Equal(t, []string{"partial", "closed", "partial", "closed", "closed", "partial"}, []string{trades[0].Status, trades[1].Status, trades[2].Status, trades[3].Status, trades[4].Status, trades[5].Status})
	assert.Equal(t, []int64{2, 2, 3, 3, 2, 2}, []int64{trades[0].Quantity, trades[1].Quantity, trades[2].Quantity, trades[3].Quantity, trades[4].Quantity, trades[5].Quantity})
	assert.Equal(t, []float64{10.0, 10.0, 10.0, 10.0, 11.0, 11.0}, []float64{trades[0].Price, trades[1].Price, trades[2].Price, trades[3].Price, trades[4].Price, trades[5].Price})

	assert.Equal(t, []float64{10.0, 10.0, 11.0}, []float64{pricePoints[0].Price, pricePoints[1].Price, pricePoints[2].Price})

	for pairIndex := 0; pairIndex < len(trades); pairIndex += 2 {
		assert.Equal(t, trades[pairIndex].TradeId, trades[pairIndex+1].TradeId)
		assert.Equal(t, "buy-in", trades[pairIndex].OrderId)
		assert.Equal(t, "BUY", trades[pairIndex].Action)
		assert.Equal(t, "SELL", trades[pairIndex+1].Action)
		assert.NotEmpty(t, trades[pairIndex].TradeId)
		assert.Greater(t, trades[pairIndex].Timestamp, int64(0))
		assert.Greater(t, trades[pairIndex+1].Timestamp, int64(0))
	}

	assert.Equal(t, 0, orderbook.BestBid.Len())
	assert.Equal(t, 1, orderbook.BestAsk.Len())
	assert.Equal(t, 11.0, orderbook.BestAsk.Peak())
	assert.Equal(t, []int64{2}, levelQuantities(orderbook.PriceToSellOrders[11.0]))
	assert.Equal(t, []string{"sell-3"}, levelOrderIDs(orderbook.PriceToSellOrders[11.0]))
}

func TestMatchingEngineProcessEventFieldsSellFlow(t *testing.T) {
	orderbook := NewOrderBook()
	matchingEngine := NewMatchingEngine(nil, orderbook)
	now := time.Now().UTC().Unix()

	restingBuy1 := Order{OrderID: "buy-1", OrderType: "limit", Price: 11.0, Quantity: 3, Action: "BUY", Timestamp: now}
	restingBuy2 := Order{OrderID: "buy-2", OrderType: "limit", Price: 10.0, Quantity: 4, Action: "BUY", Timestamp: now}
	matchingEngine.Process(&restingBuy1, nil, nil)
	matchingEngine.Process(&restingBuy2, nil, nil)

	tradeChannel := make(chan Trade, 12)
	pricePointChannel := make(chan PricePoint, 12)
	incomingSell := Order{OrderID: "sell-in", OrderType: "limit", Price: 10.0, Quantity: 5, Action: "SELL", Timestamp: now}
	matchingEngine.Process(&incomingSell, tradeChannel, pricePointChannel)

	trades := collectTrades(tradeChannel, 4)
	pricePoints := collectPricePoints(pricePointChannel, 2)

	assert.Equal(t, []string{"sell-in", "buy-1", "sell-in", "buy-2"}, []string{trades[0].OrderId, trades[1].OrderId, trades[2].OrderId, trades[3].OrderId})
	assert.Equal(t, []string{"SELL", "BUY", "SELL", "BUY"}, []string{trades[0].Action, trades[1].Action, trades[2].Action, trades[3].Action})
	assert.Equal(t, []string{"partial", "closed", "closed", "partial"}, []string{trades[0].Status, trades[1].Status, trades[2].Status, trades[3].Status})
	assert.Equal(t, []int64{3, 3, 2, 2}, []int64{trades[0].Quantity, trades[1].Quantity, trades[2].Quantity, trades[3].Quantity})
	assert.Equal(t, []float64{11.0, 11.0, 10.0, 10.0}, []float64{trades[0].Price, trades[1].Price, trades[2].Price, trades[3].Price})

	assert.Equal(t, []float64{11.0, 10.0}, []float64{pricePoints[0].Price, pricePoints[1].Price})

	for pairIndex := 0; pairIndex < len(trades); pairIndex += 2 {
		assert.Equal(t, trades[pairIndex].TradeId, trades[pairIndex+1].TradeId)
		assert.Equal(t, "sell-in", trades[pairIndex].OrderId)
		assert.Equal(t, "SELL", trades[pairIndex].Action)
		assert.Equal(t, "BUY", trades[pairIndex+1].Action)
		assert.NotEmpty(t, trades[pairIndex].TradeId)
		assert.Greater(t, trades[pairIndex].Timestamp, int64(0))
		assert.Greater(t, trades[pairIndex+1].Timestamp, int64(0))
	}

	assert.Equal(t, 1, orderbook.BestBid.Len())
	assert.Equal(t, 10.0, orderbook.BestBid.Peak())
	assert.Equal(t, 0, orderbook.BestAsk.Len())
	assert.Equal(t, []int64{2}, levelQuantities(orderbook.PriceToBuyOrders[10.0]))
	assert.Equal(t, []string{"buy-2"}, levelOrderIDs(orderbook.PriceToBuyOrders[10.0]))
}

func TestMatchingEngineOpenOrderCountRemainsO1AndAccurate(t *testing.T) {
	orderbook := NewOrderBook()
	matchingEngine := NewMatchingEngine(nil, orderbook)
	now := time.Now().UTC().Unix()

	restingSell1 := Order{OrderID: "sell-1", OrderType: "limit", Price: 10.0, Quantity: 5, Action: "SELL", Timestamp: now}
	restingSell2 := Order{OrderID: "sell-2", OrderType: "limit", Price: 10.0, Quantity: 3, Action: "SELL", Timestamp: now}
	matchingEngine.Process(&restingSell1, nil, nil)
	matchingEngine.Process(&restingSell2, nil, nil)
	assert.Equal(t, 2, orderbook.OpenOrderCount())

	incomingBuy := Order{OrderID: "buy-in", OrderType: "limit", Price: 10.0, Quantity: 6, Action: "BUY", Timestamp: now}
	matchingEngine.Process(&incomingBuy, nil, nil)

	assert.Equal(t, 1, orderbook.OpenOrderCount())
	assert.Equal(t, []string{"sell-2"}, levelOrderIDs(orderbook.PriceToSellOrders[10.0]))
	assert.Equal(t, []int64{2}, levelQuantities(orderbook.PriceToSellOrders[10.0]))
}
