package main

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func newTestHandler(buffer *bytes.Buffer, withColor bool) *colorTextHandler {
	return &colorTextHandler{
		writer: buffer,
		opts: loggerOptions{
			level:      slog.LevelDebug,
			timeFormat: time.RFC3339,
			withColor:  withColor,
		},
		mu: &sync.Mutex{},
	}
}

func TestColorTextHandlerIncludesTimestampMethodMessageAndOrderID(t *testing.T) {
	var buffer bytes.Buffer
	handler := newTestHandler(&buffer, false)

	recordTime := time.Date(2026, 3, 4, 12, 0, 0, 0, time.UTC)
	record := slog.NewRecord(recordTime, slog.LevelInfo, "order consumed", 0)
	record.AddAttrs(
		slog.String("method", "kafka.consume.order"),
		slog.String("order_id", "ord-123"),
		slog.Int64("offset", 42),
	)

	err := handler.Handle(context.Background(), record)
	if !assert.NoError(t, err) {
		return
	}

	output := strings.TrimSpace(buffer.String())
	assert.Contains(t, output, "time=2026-03-04T12:00:00Z")
	assert.Contains(t, output, "level=INFO")
	assert.Contains(t, output, "method=kafka.consume.order")
	assert.Contains(t, output, `msg="order consumed"`)
	assert.Contains(t, output, "order_id=ord-123")
	assert.Contains(t, output, "offset=42")
}

func TestColorTextHandlerAppliesColorByLevel(t *testing.T) {
	var buffer bytes.Buffer
	handler := newTestHandler(&buffer, true)

	record := slog.NewRecord(time.Now().UTC(), slog.LevelError, "producer failed", 0)
	record.AddAttrs(slog.String("method", "kafka.produce.trade"))

	err := handler.Handle(context.Background(), record)
	if !assert.NoError(t, err) {
		return
	}

	output := strings.TrimSpace(buffer.String())
	assert.True(t, strings.HasPrefix(output, ansiRed), "expected error line to start with red ANSI color")
	assert.True(t, strings.HasSuffix(output, ansiReset), "expected line to end with reset ANSI color")
}

func TestColorTextHandlerEmitsCorrelationID(t *testing.T) {
	var buffer bytes.Buffer
	handler := newTestHandler(&buffer, false)

	record := slog.NewRecord(time.Now().UTC(), slog.LevelInfo, "trade executed", 0)
	record.AddAttrs(
		slog.String("method", "matching.process.trade"),
		slog.String("order_id", "incoming-1"),
		slog.String("correlation_id", "trade-xyz"),
	)

	err := handler.Handle(context.Background(), record)
	if !assert.NoError(t, err) {
		return
	}

	output := strings.TrimSpace(buffer.String())
	assert.Contains(t, output, "order_id=incoming-1")
	assert.Contains(t, output, "correlation_id=trade-xyz")
}
