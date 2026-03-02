package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// utils
func cut(i int, orderSlicesRef *[]*Order) (*Order, error) {
	if i < 0 || i >= len(*orderSlicesRef) {
		return nil, fmt.Errorf("index out of bounds: %d", i)
	}
	removedOrder := (*orderSlicesRef)[i]
	*orderSlicesRef = append((*orderSlicesRef)[:i], (*orderSlicesRef)[i+1:]...)
	return removedOrder, nil
}

func getenv(key, fallback string) string {
	// TODO: refactor through a configparser
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}

func main() {
	fmt.Println("INFO: starting orderbook")
	metrics := NewEngineMetrics()
	metricsAddress := getenv("METRICS_ADDRESS", ":2112")
	metricsPath := getenv("METRICS_PATH", "/metrics")
	metricsServer := StartMetricsServer(metricsAddress, metricsPath, metrics)

	me := NewMatchingEngine(
		NewKafkaClient(),
		NewOrderBook(),
	)
	me.SetMetrics(metrics)

	shutdownSignalCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	me.Start(shutdownSignalCtx)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		log.Printf("ERROR: metrics server shutdown failed: %v", err)
	}
}
