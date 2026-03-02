package main

import (
	"strings"
	"testing"
	"time"
)

func TestGenerateAndPushOrderEmitsOneOrder(t *testing.T) {
	trader := Trader{TradeId: "Trader-1", Price: 100.0}
	orders := make(chan Order, 2)

	GenerateAndPushOrder(trader, orders)

	select {
	case <-orders:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for order")
	}

	select {
	case extra := <-orders:
		t.Fatalf("expected exactly one order, got unexpected extra: %+v", extra)
	default:
	}
}

func TestGenerateAndPushOrderInvariants(t *testing.T) {
	trader := Trader{TradeId: "Trader-2", Price: 250.0}
	orders := make(chan Order, 1)

	for i := 0; i < 100; i++ {
		GenerateAndPushOrder(trader, orders)

		select {
		case order := <-orders:
			if !strings.HasPrefix(order.OrderID, trader.TradeId+"-") {
				t.Fatalf("order id should be prefixed by trader id, got %q", order.OrderID)
			}
			if order.OrderType != "limit" {
				t.Fatalf("expected order_type=limit, got %q", order.OrderType)
			}
			if order.Quantity < 10 || order.Quantity > 39 {
				t.Fatalf("quantity out of expected range [10,39], got %d", order.Quantity)
			}
			if order.Timestamp <= 0 {
				t.Fatalf("timestamp should be positive, got %d", order.Timestamp)
			}

			switch order.Action {
			case "BUY":
				if order.Price < trader.Price+0.5 {
					t.Fatalf("buy price should be >= base+0.5, got %.4f for base %.4f", order.Price, trader.Price)
				}
			case "SELL":
				if order.Price > trader.Price-0.5 {
					t.Fatalf("sell price should be <= base-0.5, got %.4f for base %.4f", order.Price, trader.Price)
				}
			default:
				t.Fatalf("unexpected action %q", order.Action)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("timed out waiting for generated order")
		}
	}
}
