package main

import (
	"errors"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type EngineMetrics struct {
	registry *prometheus.Registry

	ConsumedOrdersCounter       prometheus.Counter
	RejectedMalformedCounter    prometheus.Counter
	ExecutedTradesCounter       prometheus.Counter
	ProducedMessagesCounter     prometheus.Counter
	BestBidGauge                prometheus.Gauge
	BestAskGauge                prometheus.Gauge
	MidPriceGauge               prometheus.Gauge
	SpreadGauge                 prometheus.Gauge
	OpenOrderCountGauge         prometheus.Gauge
	ProcessDurationHistogram    prometheus.Histogram
	PerOrderMatchCountHistogram prometheus.Histogram
}

func NewEngineMetrics() *EngineMetrics {
	registry := prometheus.NewRegistry()

	metrics := &EngineMetrics{
		registry: registry,
		ConsumedOrdersCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "orderbook",
			Subsystem: "engine",
			Name:      "consumed_orders_total",
			Help:      "Total number of consumed orders",
		}),
		RejectedMalformedCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "orderbook",
			Subsystem: "engine",
			Name:      "rejected_malformed_orders_total",
			Help:      "Total number of rejected or malformed orders",
		}),
		ExecutedTradesCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "orderbook",
			Subsystem: "engine",
			Name:      "executed_trades_total",
			Help:      "Total number of executed trades",
		}),
		ProducedMessagesCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "orderbook",
			Subsystem: "engine",
			Name:      "produced_messages_total",
			Help:      "Total number of produced output messages",
		}),
		BestBidGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "orderbook",
			Subsystem: "engine",
			Name:      "best_bid",
			Help:      "Current best bid price",
		}),
		BestAskGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "orderbook",
			Subsystem: "engine",
			Name:      "best_ask",
			Help:      "Current best ask price",
		}),
		MidPriceGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "orderbook",
			Subsystem: "engine",
			Name:      "mid_price",
			Help:      "Current mid price",
		}),
		SpreadGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "orderbook",
			Subsystem: "engine",
			Name:      "spread",
			Help:      "Current bid-ask spread",
		}),
		OpenOrderCountGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "orderbook",
			Subsystem: "engine",
			Name:      "open_order_count",
			Help:      "Current number of open orders in the book",
		}),
		ProcessDurationHistogram: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "orderbook",
			Subsystem: "engine",
			Name:      "process_duration_seconds",
			Help:      "Duration of order processing loop iterations in seconds",
			Buckets:   prometheus.DefBuckets,
		}),
		PerOrderMatchCountHistogram: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "orderbook",
			Subsystem: "engine",
			Name:      "per_order_match_count",
			Help:      "Number of matches found per consumed order",
			Buckets:   []float64{0, 1, 2, 3, 5, 8, 13, 21, 34},
		}),
	}

	registry.MustRegister(
		metrics.ConsumedOrdersCounter,
		metrics.RejectedMalformedCounter,
		metrics.ExecutedTradesCounter,
		metrics.ProducedMessagesCounter,
		metrics.BestBidGauge,
		metrics.BestAskGauge,
		metrics.MidPriceGauge,
		metrics.SpreadGauge,
		metrics.OpenOrderCountGauge,
		metrics.ProcessDurationHistogram,
		metrics.PerOrderMatchCountHistogram,
	)

	return metrics
}

func (m *EngineMetrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func StartMetricsServer(address string, path string, metrics *EngineMetrics) *http.Server {
	mux := http.NewServeMux()
	mux.Handle(path, metrics.Handler())

	server := &http.Server{
		Addr:    address,
		Handler: mux,
	}

	go func() {
		logger := logWithMethod("metrics.server")
		logger.Info("metrics server listening", "address", address, "path", path)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server failed", "address", address, "path", path, "error", err)
		}
	}()

	return server
}
