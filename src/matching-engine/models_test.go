package main

import (
	"testing"

	"orderbookpb/contracts"

	"google.golang.org/protobuf/proto"
)

func encodeOrderMessage(t *testing.T, order *contracts.Order) []byte {
	t.Helper()

	message, err := proto.Marshal(order)
	if err != nil {
		t.Fatalf("expected valid protobuf order payload: %v", err)
	}

	return message
}

func TestMessageToOrderValid(t *testing.T) {
	message := encodeOrderMessage(t, &contracts.Order{OrderId: "uuid-1", OrderType: "limit", Price: 10.5, Quantity: 7, Action: "BUY", Timestamp: 1700000000})
	order, err := messageToOrder(message)

	if err != nil {
		t.Fatalf("expected valid order message, got error: %v", err)
	}

	if order.OrderID != "uuid-1" || order.OrderType != "limit" || order.Price != 10.5 || order.Quantity != 7 || order.Timestamp != 1700000000 {
		t.Fatalf("unexpected parsed order: %+v", order)
	}
	if order.Action != "BUY" {
		t.Fatalf("expected BUY action, got %+v", order.Action)
	}
}

func TestMessageToOrderRejectsNegativeQuantity(t *testing.T) {
	message := encodeOrderMessage(t, &contracts.Order{OrderId: "uuid-3", OrderType: "limit", Price: 11.0, Quantity: -5, Action: "SELL", Timestamp: 1700000002})
	_, err := messageToOrder(message)

	if err == nil {
		t.Fatalf("expected reject for negative quantity")
	}
}

func TestMessageToOrderRejectsMissingAction(t *testing.T) {
	message := encodeOrderMessage(t, &contracts.Order{OrderId: "uuid-5", OrderType: "limit", Price: 12.0, Quantity: 2, Timestamp: 1700000004})
	_, err := messageToOrder(message)

	if err == nil {
		t.Fatalf("expected reject for missing action")
	}
}

func TestMessageToOrderRejectsZeroQuantity(t *testing.T) {
	message := encodeOrderMessage(t, &contracts.Order{OrderId: "uuid-4", OrderType: "limit", Price: 11.0, Quantity: 0, Action: "BUY", Timestamp: 1700000003})
	_, err := messageToOrder(message)

	if err == nil {
		t.Fatalf("expected reject for zero quantity")
	}
}

func TestMessageToOrderInvalidProtobuf(t *testing.T) {
	message := []byte{0xff, 0x01, 0x03}
	_, err := messageToOrder(message)

	if err == nil {
		t.Fatalf("expected parsing error for malformed message")
	}
}

func TestTradeToMessageRoundTrip(t *testing.T) {
	original := Trade{
		TradeId:   "trade-1",
		OrderId:   "order-1",
		Quantity:  3,
		Price:     11.25,
		Action:    "BUY",
		Status:    "partial",
		Timestamp: 1700000001,
	}

	message := original.toMessage()

	var decoded contracts.Trade
	if err := proto.Unmarshal(message, &decoded); err != nil {
		t.Fatalf("expected valid trade protobuf, got error: %v", err)
	}

	if !proto.Equal(tradeToProto(original), &decoded) {
		t.Fatalf("decoded trade differs: got %s want %s", (&decoded).String(), tradeToProto(original).String())
	}
}

func TestPricePointToMessageRoundTrip(t *testing.T) {
	original := PricePoint{Price: 12.75}
	message := original.toMessage()

	var decoded contracts.PricePoint
	if err := proto.Unmarshal(message, &decoded); err != nil {
		t.Fatalf("expected valid price point protobuf, got error: %v", err)
	}

	if !proto.Equal(pricePointToProto(original), &decoded) {
		t.Fatalf("decoded pricepoint differs: got %s want %s", (&decoded).String(), pricePointToProto(original).String())
	}
}

func TestCreateTradeStatusClosed(t *testing.T) {
	order := &Order{OrderID: "order-closed", Quantity: 0}
	trade := createTrade("trade-closed", order, 5, 10.0, "BUY", 1700000002)

	if trade.Status != "closed" {
		t.Fatalf("expected closed status, got %s", trade.Status)
	}
	if trade.TradeId != "trade-closed" || trade.OrderId != "order-closed" || trade.Quantity != 5 || trade.Price != 10.0 || trade.Action != "BUY" || trade.Timestamp != 1700000002 {
		t.Fatalf("unexpected trade payload: %+v", trade)
	}
}

func TestCreateTradeStatusPartial(t *testing.T) {
	order := &Order{OrderID: "order-partial", Quantity: 2}
	trade := createTrade("trade-partial", order, 3, 11.0, "SELL", 1700000003)

	if trade.Status != "partial" {
		t.Fatalf("expected partial status, got %s", trade.Status)
	}
	if trade.TradeId != "trade-partial" || trade.OrderId != "order-partial" || trade.Quantity != 3 || trade.Price != 11.0 || trade.Action != "SELL" || trade.Timestamp != 1700000003 {
		t.Fatalf("unexpected trade payload: %+v", trade)
	}
}

func TestRejectAndContinueMalformedInput(t *testing.T) {
	orderbook := NewOrderBook()
	matchingEngine := NewMatchingEngine(nil, orderbook)

	messages := [][]byte{
		encodeOrderMessage(t, &contracts.Order{OrderId: "buy-1", OrderType: "limit", Price: 10.0, Quantity: 4, Action: "BUY", Timestamp: 1700000000}),
		[]byte{0xff, 0x01, 0x03},
		encodeOrderMessage(t, &contracts.Order{OrderId: "sell-1", OrderType: "limit", Price: 11.0, Quantity: 3, Action: "SELL", Timestamp: 1700000001}),
	}

	rejectedCount := 0
	processedCount := 0
	for _, message := range messages {
		order, err := messageToOrder(message)
		if err != nil {
			rejectedCount++
			continue
		}
		matchingEngine.Process(&order, nil, nil)
		processedCount++
	}

	if rejectedCount != 1 {
		t.Fatalf("expected 1 malformed message rejected, got %d", rejectedCount)
	}
	if processedCount != 2 {
		t.Fatalf("expected 2 valid messages processed, got %d", processedCount)
	}

	if orderbook.BestBid.Len() != 1 || orderbook.BestBid.Peak() != 10.0 {
		t.Fatalf("unexpected best bid state after reject-and-continue: len=%d peak=%v", orderbook.BestBid.Len(), orderbook.BestBid.Peak())
	}
	if orderbook.BestAsk.Len() != 1 || orderbook.BestAsk.Peak() != 11.0 {
		t.Fatalf("unexpected best ask state after reject-and-continue: len=%d peak=%v", orderbook.BestAsk.Len(), orderbook.BestAsk.Peak())
	}

	bidOrders := orderbook.PriceToBuyOrders[10.0]
	bidFront := (*Order)(nil)
	if bidOrders != nil {
		bidFront = bidOrders.PeekFront()
	}
	if bidOrders == nil || bidOrders.Len() != 1 || bidFront == nil || bidFront.OrderID != "buy-1" || bidFront.Quantity != 4 {
		t.Fatalf("unexpected bid queue after reject-and-continue: %+v", bidOrders)
	}

	askOrders := orderbook.PriceToSellOrders[11.0]
	askFront := (*Order)(nil)
	if askOrders != nil {
		askFront = askOrders.PeekFront()
	}
	if askOrders == nil || askOrders.Len() != 1 || askFront == nil || askFront.OrderID != "sell-1" || askFront.Quantity != 3 {
		t.Fatalf("unexpected ask queue after reject-and-continue: %+v", askOrders)
	}
}

func TestRejectZeroQuantityOrderNoSideEffects(t *testing.T) {
	orderbook := NewOrderBook()
	matchingEngine := NewMatchingEngine(nil, orderbook)

	restingBuy := Order{OrderID: "resting-buy", OrderType: "limit", Price: 10.0, Quantity: 3, Action: "BUY", Timestamp: 1700000000}
	restingSell := Order{OrderID: "resting-sell", OrderType: "limit", Price: 11.0, Quantity: 4, Action: "SELL", Timestamp: 1700000001}
	matchingEngine.Process(&restingBuy, nil, nil)
	matchingEngine.Process(&restingSell, nil, nil)

	tradeChannel := make(chan Trade, 4)
	pricePointChannel := make(chan PricePoint, 4)
	zeroQtyOrder := Order{OrderID: "zero-order", OrderType: "limit", Price: 11.0, Quantity: 0, Action: "SELL", Timestamp: 1700000002}
	matchingEngine.Process(&zeroQtyOrder, tradeChannel, pricePointChannel)

	if len(tradeChannel) != 0 {
		t.Fatalf("expected no trade events for zero-quantity order, got %d", len(tradeChannel))
	}
	if len(pricePointChannel) != 0 {
		t.Fatalf("expected no pricepoint events for zero-quantity order, got %d", len(pricePointChannel))
	}

	if orderbook.BestBid.Len() != 1 || orderbook.BestBid.Peak() != 10.0 {
		t.Fatalf("unexpected best bid after zero-quantity order: len=%d peak=%v", orderbook.BestBid.Len(), orderbook.BestBid.Peak())
	}
	if orderbook.BestAsk.Len() != 1 || orderbook.BestAsk.Peak() != 11.0 {
		t.Fatalf("unexpected best ask after zero-quantity order: len=%d peak=%v", orderbook.BestAsk.Len(), orderbook.BestAsk.Peak())
	}

	bidOrders := orderbook.PriceToBuyOrders[10.0]
	bidFront := (*Order)(nil)
	if bidOrders != nil {
		bidFront = bidOrders.PeekFront()
	}
	if bidOrders == nil || bidOrders.Len() != 1 || bidFront == nil || bidFront.OrderID != "resting-buy" || bidFront.Quantity != 3 {
		t.Fatalf("unexpected bid queue after zero-quantity order: %+v", bidOrders)
	}

	askOrders := orderbook.PriceToSellOrders[11.0]
	askFront := (*Order)(nil)
	if askOrders != nil {
		askFront = askOrders.PeekFront()
	}
	if askOrders == nil || askOrders.Len() != 1 || askFront == nil || askFront.OrderID != "resting-sell" || askFront.Quantity != 4 {
		t.Fatalf("unexpected ask queue after zero-quantity order: %+v", askOrders)
	}
}
