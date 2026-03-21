package main

import (
	"errors"
	"log"
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
	BestBidQuantityGauge        prometheus.Gauge
	BestAskQuantityGauge        prometheus.Gauge
	MidPriceGauge               prometheus.Gauge
	SpreadGauge                 prometheus.Gauge
	SpreadBpsGauge              prometheus.Gauge
	NearMidDepthQuantityGauge   *prometheus.GaugeVec
	TwoSidedBookGauge           prometheus.Gauge
	MidPriceJumpAbsGauge        prometheus.Gauge
	OpenOrderCountGauge         prometheus.Gauge
	SubmittedQuantityCounter    prometheus.Counter
	ExecutedQuantityCounter     prometheus.Counter
	ProcessDurationHistogram    prometheus.Histogram
	PerOrderMatchCountHistogram prometheus.Histogram
	TimeToFirstFillHistogram    prometheus.Histogram
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
		BestBidQuantityGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "orderbook",
			Subsystem: "engine",
			Name:      "best_bid_quantity",
			Help:      "Current aggregate quantity at best bid price level",
		}),
		BestAskQuantityGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "orderbook",
			Subsystem: "engine",
			Name:      "best_ask_quantity",
			Help:      "Current aggregate quantity at best ask price level",
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
		SpreadBpsGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "orderbook",
			Subsystem: "engine",
			Name:      "spread_bps",
			Help:      "Current bid-ask spread in basis points",
		}),
		NearMidDepthQuantityGauge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "orderbook",
			Subsystem: "engine",
			Name:      "near_mid_depth_quantity",
			Help:      "Aggregate resting quantity near mid-price by side and band",
		}, []string{"side", "band_bps"}),
		TwoSidedBookGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "orderbook",
			Subsystem: "engine",
			Name:      "two_sided_book",
			Help:      "Whether both bid and ask are present (1=true, 0=false)",
		}),
		MidPriceJumpAbsGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "orderbook",
			Subsystem: "engine",
			Name:      "mid_price_jump_abs",
			Help:      "Absolute jump in mid-price versus last observed valid mid-price",
		}),
		OpenOrderCountGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "orderbook",
			Subsystem: "engine",
			Name:      "open_order_count",
			Help:      "Current number of open orders in the book",
		}),
		SubmittedQuantityCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "orderbook",
			Subsystem: "engine",
			Name:      "submitted_quantity_total",
			Help:      "Total accepted resting submitted quantity (maker-side denominator)",
		}),
		ExecutedQuantityCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "orderbook",
			Subsystem: "engine",
			Name:      "executed_quantity_total",
			Help:      "Total executed resting quantity (maker-side numerator)",
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
		TimeToFirstFillHistogram: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "orderbook",
			Subsystem: "engine",
			Name:      "time_to_first_fill_seconds",
			Help:      "Latency from accepted resting order ingest to first fill",
			Buckets:   []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
		}),
	}

	for _, side := range []string{"bid", "ask"} {
		for _, bandBps := range []string{"25", "50", "100"} {
			metrics.NearMidDepthQuantityGauge.WithLabelValues(side, bandBps).Set(0)
		}
	}

	registry.MustRegister(
		metrics.ConsumedOrdersCounter,
		metrics.RejectedMalformedCounter,
		metrics.ExecutedTradesCounter,
		metrics.ProducedMessagesCounter,
		metrics.BestBidGauge,
		metrics.BestAskGauge,
		metrics.BestBidQuantityGauge,
		metrics.BestAskQuantityGauge,
		metrics.MidPriceGauge,
		metrics.SpreadGauge,
		metrics.SpreadBpsGauge,
		metrics.NearMidDepthQuantityGauge,
		metrics.TwoSidedBookGauge,
		metrics.MidPriceJumpAbsGauge,
		metrics.OpenOrderCountGauge,
		metrics.SubmittedQuantityCounter,
		metrics.ExecutedQuantityCounter,
		metrics.ProcessDurationHistogram,
		metrics.PerOrderMatchCountHistogram,
		metrics.TimeToFirstFillHistogram,
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
		log.Printf("INFO: metrics server listening on %s%s", address, path)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("ERROR: metrics server failed: %v", err)
		}
	}()

	return server
}
