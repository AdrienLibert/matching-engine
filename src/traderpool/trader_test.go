package main

import (
	"strings"
	"testing"
	"time"
)

func TestGenerateAndPushOrderEmitsOneOrder(t *testing.T) {
	trader := Trader{TradeId: "Trader-1", Price: 100.0}
	orders := make(chan Order, 2)

	GenerateAndPushRandomOrder(trader, orders)

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
		GenerateAndPushRandomOrder(trader, orders)

		select {
		case order := <-orders:
			if !strings.HasPrefix(order.OrderID, "RANDOM-") {
				t.Fatalf("order id should be prefixed by RANDOM-, got %q", order.OrderID)
			}
			if order.OrderType != "limit" {
				t.Fatalf("expected order_type=limit, got %q", order.OrderType)
			}
			if order.Quantity <= 0 {
				t.Fatalf("quantity should be positive, got %d", order.Quantity)
			}
			if order.Timestamp <= 0 {
				t.Fatalf("timestamp should be positive, got %d", order.Timestamp)
			}

			switch order.Action {
			case "BUY":
				if order.Price > trader.Price {
					t.Fatalf("buy price should be <= base price, got %.4f for base %.4f", order.Price, trader.Price)
				}
			case "SELL":
				if order.Price < trader.Price {
					t.Fatalf("sell price should be >= base price, got %.4f for base %.4f", order.Price, trader.Price)
				}
			default:
				t.Fatalf("unexpected action %q", order.Action)
			}
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("timed out waiting for generated order")
		}
	}
}

func TestGenerateMeanReversionOrderCanEmitWithDistinctReferenceAndMarketPrice(t *testing.T) {
	trader := Trader{TradeId: "Trader-3", Price: 100.0}
	orders := make(chan Order, 1)

	GenerateMeanReversionOrder(trader, 103.0, orders)

	select {
	case order := <-orders:
		if !strings.HasPrefix(order.OrderID, "ARBITRAGE-") {
			t.Fatalf("order id should be prefixed by ARBITRAGE-, got %q", order.OrderID)
		}
		if order.Action != "SELL" {
			t.Fatalf("expected SELL for upward deviation, got %q", order.Action)
		}
		if order.Price != trader.Price {
			t.Fatalf("mean-reversion should place at reference price %.2f, got %.2f", trader.Price, order.Price)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timed out waiting for mean-reversion order")
	}
}
