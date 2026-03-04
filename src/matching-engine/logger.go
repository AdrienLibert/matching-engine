package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	ansiReset  = "\x1b[0m"
	ansiRed    = "\x1b[31m"
	ansiYellow = "\x1b[33m"
	ansiBlue   = "\x1b[34m"

	defaultMethodName = "matching_engine"
)

var engineLogger = newMatchingEngineLogger(os.Stdout)

func newMatchingEngineLogger(writer io.Writer) *slog.Logger {
	if writer == nil {
		writer = os.Stdout
	}

	logLevel := parseLogLevel(getenv("OB__MATCHING_ENGINE__LOG_LEVEL", "info"))
	handler := &colorTextHandler{
		writer: writer,
		opts: loggerOptions{
			level:      logLevel,
			timeFormat: time.RFC3339,
			withColor:  shouldUseColor(),
		},
		mu: &sync.Mutex{},
	}

	return slog.New(handler)
}

func setEngineLogger(logger *slog.Logger) {
	if logger == nil {
		return
	}
	engineLogger = logger
}

func logWithMethod(method string) *slog.Logger {
	if method == "" {
		method = defaultMethodName
	}
	return engineLogger.With("method", method)
}

func parseLogLevel(raw string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func shouldUseColor() bool {
	if strings.EqualFold(getenv("NO_COLOR", ""), "1") {
		return false
	}
	if strings.EqualFold(getenv("OB__MATCHING_ENGINE__LOG_COLOR", "true"), "false") {
		return false
	}
	if strings.EqualFold(getenv("TERM", ""), "dumb") {
		return false
	}
	return true
}

type loggerOptions struct {
	level      slog.Leveler
	timeFormat string
	withColor  bool
}

type colorTextHandler struct {
	writer io.Writer
	opts   loggerOptions
	attrs  []slog.Attr
	groups []string
	mu     *sync.Mutex
}

func (handler *colorTextHandler) Enabled(_ context.Context, level slog.Level) bool {
	if handler.opts.level == nil {
		return true
	}
	return level >= handler.opts.level.Level()
}

func (handler *colorTextHandler) Handle(_ context.Context, record slog.Record) error {
	flattenedAttrs := make([]slog.Attr, 0, len(handler.attrs)+record.NumAttrs())
	flattenedAttrs = append(flattenedAttrs, handler.attrs...)
	record.Attrs(func(attr slog.Attr) bool {
		flattenedAttrs = append(flattenedAttrs, attr)
		return true
	})

	method := defaultMethodName
	orderID := ""
	correlationID := ""
	extraFields := make([]string, 0, len(flattenedAttrs))

	for _, attr := range flattenedAttrs {
		handler.collectAttr(attr, "", &method, &orderID, &correlationID, &extraFields)
	}

	timestamp := record.Time
	if timestamp.IsZero() {
		timestamp = time.Now()
	}

	parts := []string{
		"time=" + timestamp.UTC().Format(handler.opts.timeFormat),
		"level=" + strings.ToUpper(record.Level.String()),
		"method=" + method,
		"msg=" + strconv.Quote(record.Message),
	}
	if orderID != "" {
		parts = append(parts, "order_id="+quoteValue(orderID))
	}
	if correlationID != "" {
		parts = append(parts, "correlation_id="+quoteValue(correlationID))
	}
	parts = append(parts, extraFields...)

	line := strings.Join(parts, " ")
	if handler.opts.withColor {
		line = colorize(record.Level, line)
	}

	handler.mu.Lock()
	defer handler.mu.Unlock()
	_, err := io.WriteString(handler.writer, line+"\n")
	return err
}

func (handler *colorTextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	newAttrs := make([]slog.Attr, 0, len(handler.attrs)+len(attrs))
	newAttrs = append(newAttrs, handler.attrs...)
	newAttrs = append(newAttrs, attrs...)

	return &colorTextHandler{
		writer: handler.writer,
		opts:   handler.opts,
		attrs:  newAttrs,
		groups: append([]string(nil), handler.groups...),
		mu:     handler.mu,
	}
}

func (handler *colorTextHandler) WithGroup(name string) slog.Handler {
	newGroups := append(append([]string(nil), handler.groups...), name)
	return &colorTextHandler{
		writer: handler.writer,
		opts:   handler.opts,
		attrs:  append([]slog.Attr(nil), handler.attrs...),
		groups: newGroups,
		mu:     handler.mu,
	}
}

func (handler *colorTextHandler) collectAttr(attr slog.Attr, parentKey string, method *string, orderID *string, correlationID *string, extras *[]string) {
	attr.Value = attr.Value.Resolve()
	if attr.Equal(slog.Attr{}) {
		return
	}

	key := attr.Key
	if key == "" {
		key = parentKey
	}
	if len(handler.groups) > 0 {
		groupPrefix := strings.Join(handler.groups, ".")
		if key != "" {
			key = groupPrefix + "." + key
		} else {
			key = groupPrefix
		}
	}
	if parentKey != "" && attr.Key != "" {
		key = parentKey + "." + attr.Key
	}

	if attr.Value.Kind() == slog.KindGroup {
		for _, nested := range attr.Value.Group() {
			handler.collectAttr(nested, key, method, orderID, correlationID, extras)
		}
		return
	}

	value := fmt.Sprintf("%v", attr.Value.Any())
	switch key {
	case "method":
		if value != "" {
			*method = value
		}
	case "order_id":
		if value != "" {
			*orderID = value
		}
	case "correlation_id":
		if value != "" {
			*correlationID = value
		}
	default:
		if key != "" {
			*extras = append(*extras, key+"="+quoteValue(value))
		}
	}
}

func quoteValue(raw string) string {
	if raw == "" {
		return `""`
	}
	if strings.ContainsAny(raw, " \t\n\r\"") {
		return strconv.Quote(raw)
	}
	return raw
}

func colorize(level slog.Level, line string) string {
	color := ansiBlue
	switch {
	case level >= slog.LevelError:
		color = ansiRed
	case level >= slog.LevelWarn:
		color = ansiYellow
	}
	return color + line + ansiReset
}
