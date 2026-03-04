package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// utils

func getenv(key, fallback string) string {
	// TODO: refactor through a configparser
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}

func main() {
	logWithMethod("service.lifecycle").Info("matching engine starting")
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
		logWithMethod("metrics.server.shutdown").Error("metrics server shutdown failed", "error", err)
	}
	logWithMethod("service.lifecycle").Info("matching engine stopped")
}
