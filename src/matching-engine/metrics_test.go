package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewEngineMetricsInitializesRegistryAndCollectors(t *testing.T) {
	metrics := NewEngineMetrics()

	assert.NotNil(t, metrics)
	assert.NotNil(t, metrics.registry)

	assert.NotNil(t, metrics.ConsumedOrdersCounter)
	assert.NotNil(t, metrics.RejectedMalformedCounter)
	assert.NotNil(t, metrics.ExecutedTradesCounter)
	assert.NotNil(t, metrics.ProducedMessagesCounter)
	assert.NotNil(t, metrics.BestBidGauge)
	assert.NotNil(t, metrics.BestAskGauge)
	assert.NotNil(t, metrics.MidPriceGauge)
	assert.NotNil(t, metrics.SpreadGauge)
	assert.NotNil(t, metrics.OpenOrderCountGauge)
	assert.NotNil(t, metrics.ProcessDurationHistogram)
	assert.NotNil(t, metrics.PerOrderMatchCountHistogram)

	metricFamilies, err := metrics.registry.Gather()
	if !assert.NoError(t, err) {
		return
	}

	assert.Len(t, metricFamilies, 11)

	familyNames := make(map[string]bool, len(metricFamilies))
	for _, family := range metricFamilies {
		familyNames[family.GetName()] = true
	}

	expectedNames := []string{
		"orderbook_engine_consumed_orders_total",
		"orderbook_engine_rejected_malformed_orders_total",
		"orderbook_engine_executed_trades_total",
		"orderbook_engine_produced_messages_total",
		"orderbook_engine_best_bid",
		"orderbook_engine_best_ask",
		"orderbook_engine_mid_price",
		"orderbook_engine_spread",
		"orderbook_engine_open_order_count",
		"orderbook_engine_process_duration_seconds",
		"orderbook_engine_per_order_match_count",
	}

	for _, metricName := range expectedNames {
		assert.True(t, familyNames[metricName], "missing metric family %s", metricName)
	}
}

func TestEngineMetricsHandlerExposesPrometheusTextOutput(t *testing.T) {
	metrics := NewEngineMetrics()

	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	recorder := httptest.NewRecorder()

	metrics.Handler().ServeHTTP(recorder, request)

	assert.Equal(t, http.StatusOK, recorder.Code)
	assert.Contains(t, recorder.Header().Get("Content-Type"), "text/plain")

	body := recorder.Body.String()
	expectedNames := []string{
		"orderbook_engine_consumed_orders_total",
		"orderbook_engine_rejected_malformed_orders_total",
		"orderbook_engine_executed_trades_total",
		"orderbook_engine_produced_messages_total",
		"orderbook_engine_best_bid",
		"orderbook_engine_best_ask",
		"orderbook_engine_mid_price",
		"orderbook_engine_spread",
		"orderbook_engine_open_order_count",
		"orderbook_engine_process_duration_seconds",
		"orderbook_engine_per_order_match_count",
	}

	for _, metricName := range expectedNames {
		assert.Contains(t, body, metricName)
	}
}

func TestStartMetricsServerServesCustomPathAndShutsDownCleanly(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if !assert.NoError(t, err) {
		return
	}
	address := listener.Addr().String()
	assert.NoError(t, listener.Close())

	metrics := NewEngineMetrics()
	path := "/custom-metrics"
	server := StartMetricsServer(address, path, metrics)

	client := &http.Client{Timeout: 250 * time.Millisecond}
	url := "http://" + address + path

	var response *http.Response
	for attempt := 0; attempt < 50; attempt++ {
		response, err = client.Get(url)
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !assert.NoError(t, err) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		return
	}
	defer response.Body.Close()

	assert.Equal(t, http.StatusOK, response.StatusCode)
	body, readErr := io.ReadAll(response.Body)
	if !assert.NoError(t, readErr) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
		return
	}
	assert.Contains(t, string(body), "orderbook_engine_consumed_orders_total")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	assert.NoError(t, server.Shutdown(ctx))

	var shutdownErr error
	for attempt := 0; attempt < 20; attempt++ {
		postShutdownResponse, requestErr := client.Get(url)
		if requestErr != nil {
			shutdownErr = requestErr
			break
		}
		postShutdownResponse.Body.Close()
		time.Sleep(20 * time.Millisecond)
	}
	assert.Error(t, shutdownErr)
}
