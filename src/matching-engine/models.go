package main

import (
	"fmt"
	"orderbookpb/contracts"
	"strings"

	"google.golang.org/protobuf/proto"
)

type Order struct {
	OrderID   string  `json:"order_id"`
	OrderType string  `json:"order_type"`
	Price     float64 `json:"price"`
	Quantity  int64   `json:"quantity"`
	Action    string  `json:"action"`
	Timestamp int64   `json:"timestamp"`
}

type Trade struct {
	TradeId   string  `json:"trade_id"`
	OrderId   string  `json:"order_id"`
	Quantity  int64   `json:"quantity"`
	Price     float64 `json:"price"`
	Action    string  `json:"action"`
	Status    string  `json:"status"`
	Timestamp int64   `json:"timestamp"`
}

type PricePoint struct {
	Price float64 `json:"price"`
}

func (trade Trade) toMessage() []byte {
	message, err := proto.Marshal(tradeToProto(trade))
	if err != nil {
		fmt.Println("ERROR: invalid trade being converted to protobuf message:", err)
	}
	return message
}

func (pricePoint PricePoint) toMessage() []byte {
	message, err := proto.Marshal(pricePointToProto(pricePoint))
	if err != nil {
		fmt.Println("ERROR: invalid price point being converted to protobuf message:", err)
	}
	return message
}

func messageToOrder(messageValue []byte) (Order, error) {
	var wireOrder contracts.Order
	if err := proto.Unmarshal(messageValue, &wireOrder); err != nil {
		return Order{}, err
	}

	order := fromProtoOrder(&wireOrder)

	if order.Quantity <= 0 {
		return Order{}, fmt.Errorf("invalid order quantity: must be > 0")
	}

	order.Action = strings.ToUpper(strings.TrimSpace(order.Action))
	if order.Action != "BUY" && order.Action != "SELL" {
		return Order{}, fmt.Errorf("invalid order action: %s", order.Action)
	}

	return order, nil
}

func fromProtoOrder(order *contracts.Order) Order {
	if order == nil {
		return Order{}
	}

	return Order{
		OrderID:   order.OrderId,
		OrderType: order.OrderType,
		Price:     order.Price,
		Quantity:  order.Quantity,
		Action:    order.Action,
		Timestamp: order.Timestamp,
	}
}

func tradeToProto(trade Trade) *contracts.Trade {
	return &contracts.Trade{
		TradeId:   trade.TradeId,
		OrderId:   trade.OrderId,
		Quantity:  trade.Quantity,
		Price:     trade.Price,
		Action:    trade.Action,
		Status:    trade.Status,
		Timestamp: trade.Timestamp,
	}
}

func pricePointToProto(pricePoint PricePoint) *contracts.PricePoint {
	return &contracts.PricePoint{Price: pricePoint.Price}
}

func createTrade(tradeId string, inOrder *Order, tradeQuantity int64, price float64, action string, ts int64) Trade {
	var status string

	if inOrder.Quantity == 0 {
		status = "closed"
	} else {
		status = "partial"
	}

	trade := Trade{
		TradeId:   tradeId,
		OrderId:   inOrder.OrderID,
		Quantity:  tradeQuantity,
		Price:     price,
		Action:    action,
		Status:    status,
		Timestamp: ts,
	}

	return trade
}

func createPricePoint(price float64) PricePoint {
	return PricePoint{
		Price: price,
	}
}
