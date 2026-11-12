package slogx_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/starsrc/pkg/observability/slogx"
)

type mockLogConfig struct {
	level            slog.Level
	format           string
	message          string
	recordTraceFlags bool
	sampledOnly      bool
	addSource        bool
}

func (c *mockLogConfig) SlogLevel() slog.Level  { return c.level }
func (c *mockLogConfig) Format() string         { return c.format }
func (c *mockLogConfig) Message() string        { return c.message }
func (c *mockLogConfig) RecordTraceFlags() bool { return c.recordTraceFlags }
func (c *mockLogConfig) SampledOnly() bool      { return c.sampledOnly }
func (c *mockLogConfig) AddSource() bool        { return c.addSource }

var _ slogx.Config = (*mockLogConfig)(nil)

func extractBaseHandler(h slog.Handler) slog.Handler {
	if otelH, ok := h.(*slogx.OtelHandler); ok {
		return otelH.Handler()
	}
	return h
}

func TestNew_FormatAndLevel(t *testing.T) {
	origDefault := slog.Default()
	t.Cleanup(func() {
		slog.SetDefault(origDefault)
	})

	t.Run("nil config defaults to info and json format", func(t *testing.T) {
		logger := slogx.New(nil)
		require.NotNil(t, logger)
		assert.True(t, logger.Enabled(context.Background(), slog.LevelInfo))
		assert.False(t, logger.Enabled(context.Background(), slog.LevelDebug))
		assert.Same(t, logger, slog.Default())

		baseHandler := extractBaseHandler(logger.Handler())
		assert.IsType(t, (*slog.JSONHandler)(nil), baseHandler, "默认应使用 JSONHandler")
	})

	t.Run("debug level and text format", func(t *testing.T) {
		cfg := &mockLogConfig{
			level:  slog.LevelDebug,
			format: "text",
		}
		logger := slogx.New(cfg)
		require.NotNil(t, logger)
		assert.True(t, logger.Enabled(context.Background(), slog.LevelDebug))

		baseHandler := extractBaseHandler(logger.Handler())
		assert.IsType(t, (*slog.TextHandler)(nil), baseHandler, "format='text' 时应使用 TextHandler")
	})

	t.Run("uppercase TEXT format uses text handler", func(t *testing.T) {
		cfg := &mockLogConfig{
			level:  slog.LevelInfo,
			format: "TEXT",
		}
		logger := slogx.New(cfg)
		require.NotNil(t, logger)

		baseHandler := extractBaseHandler(logger.Handler())
		assert.IsType(t, (*slog.TextHandler)(nil), baseHandler, "format='TEXT' 时应不区分大小写匹配 TextHandler")
	})

	t.Run("warn level and json format", func(t *testing.T) {
		cfg := &mockLogConfig{
			level:  slog.LevelWarn,
			format: "json",
		}
		logger := slogx.New(cfg)
		require.NotNil(t, logger)
		assert.False(t, logger.Enabled(context.Background(), slog.LevelInfo))
		assert.True(t, logger.Enabled(context.Background(), slog.LevelWarn))
		assert.True(t, logger.Enabled(context.Background(), slog.LevelError))

		baseHandler := extractBaseHandler(logger.Handler())
		assert.IsType(t, (*slog.JSONHandler)(nil), baseHandler, "format='json' 时应使用 JSONHandler")
	})

	t.Run("WithWriter writes to custom io.Writer and WithSetDefault(false) preserves global default", func(t *testing.T) {
		sentinelDefault := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))
		slog.SetDefault(sentinelDefault)

		var customBuf bytes.Buffer
		cfg := &mockLogConfig{
			level:     slog.LevelInfo,
			format:    "json",
			addSource: true,
		}
		logger := slogx.New(cfg, slogx.WithWriter(&customBuf), slogx.WithSetDefault(false))
		require.NotNil(t, logger)
		assert.Same(t, sentinelDefault, slog.Default(), "WithSetDefault(false) 不得篡改全局 slog.Default()")

		logger.Info("custom writer message", "k", "v")
		var logMap map[string]any
		require.NoError(t, json.Unmarshal(customBuf.Bytes(), &logMap))
		assert.Equal(t, "custom writer message", logMap["msg"])
		assert.Equal(t, "v", logMap["k"])
		assert.NotNil(t, logMap["source"], "addSource=true 时日志应包含 source 属性")
	})
}

func TestOtelHandler_TraceInjection(t *testing.T) {
	var buf bytes.Buffer
	baseHandler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})

	t.Run("injects trace_id and span_id when span is present", func(t *testing.T) {
		buf.Reset()
		cfg := &mockLogConfig{
			recordTraceFlags: true,
			sampledOnly:      false,
		}
		handler := slogx.NewOtelHandler(baseHandler, cfg)
		logger := slog.New(handler)

		tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
		tracer := tp.Tracer("test-tracer")
		ctx, span := tracer.Start(context.Background(), "test-span")
		defer span.End()

		logger.InfoContext(ctx, "hello trace")

		var logMap map[string]any
		err := json.Unmarshal(buf.Bytes(), &logMap)
		require.NoError(t, err)

		sc := span.SpanContext()
		assert.Equal(t, sc.TraceID().String(), logMap[slogx.DefaultTraceIDKey])
		assert.Equal(t, sc.SpanID().String(), logMap[slogx.DefaultSpanIDKey])
		assert.Equal(t, sc.TraceFlags().String(), logMap[slogx.DefaultTraceFlagsKey])
		assert.Equal(t, "hello trace", logMap["msg"])
	})

	t.Run("no trace attributes when context has no span", func(t *testing.T) {
		buf.Reset()
		handler := slogx.NewOtelHandler(baseHandler, nil)
		logger := slog.New(handler)

		logger.InfoContext(context.Background(), "no trace here")

		var logMap map[string]any
		err := json.Unmarshal(buf.Bytes(), &logMap)
		require.NoError(t, err)

		assert.Nil(t, logMap[slogx.DefaultTraceIDKey])
		assert.Nil(t, logMap[slogx.DefaultSpanIDKey])
	})

	t.Run("sampledOnly true skips unsampled span", func(t *testing.T) {
		buf.Reset()
		cfg := &mockLogConfig{
			recordTraceFlags: false,
			sampledOnly:      true,
		}
		handler := slogx.NewOtelHandler(baseHandler, cfg)
		logger := slog.New(handler)

		tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.NeverSample()))
		tracer := tp.Tracer("test-tracer")
		ctx, span := tracer.Start(context.Background(), "unsampled-span")
		defer span.End()

		logger.InfoContext(ctx, "unsampled msg")

		var logMap map[string]any
		err := json.Unmarshal(buf.Bytes(), &logMap)
		require.NoError(t, err)

		assert.Nil(t, logMap[slogx.DefaultTraceIDKey])
		assert.Nil(t, logMap[slogx.DefaultSpanIDKey])
	})

	t.Run("sampledOnly false injects unsampled span", func(t *testing.T) {
		buf.Reset()
		cfg := &mockLogConfig{
			recordTraceFlags: false,
			sampledOnly:      false,
		}
		handler := slogx.NewOtelHandler(baseHandler, cfg)
		logger := slog.New(handler)

		tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.NeverSample()))
		tracer := tp.Tracer("test-tracer")
		ctx, span := tracer.Start(context.Background(), "unsampled-span")
		defer span.End()

		logger.InfoContext(ctx, "unsampled msg")

		var logMap map[string]any
		err := json.Unmarshal(buf.Bytes(), &logMap)
		require.NoError(t, err)

		sc := span.SpanContext()
		assert.Equal(t, sc.TraceID().String(), logMap[slogx.DefaultTraceIDKey])
		assert.Equal(t, sc.SpanID().String(), logMap[slogx.DefaultSpanIDKey])
		assert.Nil(t, logMap[slogx.DefaultTraceFlagsKey])
	})

	t.Run("WithAttrs and WithGroup preserve trace injection", func(t *testing.T) {
		buf.Reset()
		handler := slogx.NewOtelHandler(baseHandler, nil)
		subLogger := slog.New(handler).With("env", "test").WithGroup("req")

		tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
		tracer := tp.Tracer("test-tracer")
		ctx, span := tracer.Start(context.Background(), "test-span")
		defer span.End()

		subLogger.InfoContext(ctx, "nested msg", "field", "val")

		output := buf.String()
		assert.Contains(t, output, span.SpanContext().TraceID().String())
		assert.Contains(t, output, `"env":"test"`)
		assert.Contains(t, output, `"req":{`)
	})

	t.Run("WithAttrs and WithGroup preserve recordTraceFlags and sampledOnly configs", func(t *testing.T) {
		cfg := &mockLogConfig{
			recordTraceFlags: true,
			sampledOnly:      true,
		}
		handler := slogx.NewOtelHandler(baseHandler, cfg)
		rootLogger := slog.New(handler)

		cases := []struct {
			name   string
			logger *slog.Logger
		}{
			{
				name:   "WithAttrs only",
				logger: rootLogger.With("component", "auth"),
			},
			{
				name:   "WithGroup only",
				logger: rootLogger.WithGroup("audit"),
			},
			{
				name:   "WithAttrs and WithGroup combined",
				logger: rootLogger.With("env", "test").WithGroup("req"),
			},
		}

		sampledTP := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
		sampledTracer := sampledTP.Tracer("test-tracer-sampled")
		unsampledTP := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.NeverSample()))
		unsampledTracer := unsampledTP.Tracer("test-tracer-unsampled")

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				buf.Reset()
				sCtx, sSpan := sampledTracer.Start(context.Background(), "sampled-span")
				tc.logger.InfoContext(sCtx, "sampled message")
				sSpan.End()

				sOutput := buf.String()
				sSC := sSpan.SpanContext()
				assert.Contains(t, sOutput, `"`+slogx.DefaultTraceIDKey+`":"`+sSC.TraceID().String()+`"`)
				assert.Contains(t, sOutput, `"`+slogx.DefaultSpanIDKey+`":"`+sSC.SpanID().String()+`"`)
				assert.Contains(t, sOutput, `"`+slogx.DefaultTraceFlagsKey+`":"`+sSC.TraceFlags().String()+`"`)

				buf.Reset()
				uCtx, uSpan := unsampledTracer.Start(context.Background(), "unsampled-span")
				tc.logger.InfoContext(uCtx, "unsampled message")
				uSpan.End()

				uOutput := buf.String()
				assert.NotContains(t, uOutput, slogx.DefaultTraceIDKey)
				assert.NotContains(t, uOutput, slogx.DefaultSpanIDKey)
				assert.NotContains(t, uOutput, slogx.DefaultTraceFlagsKey)
			})
		}
	})

	t.Run("Enabled delegates to next handler", func(t *testing.T) {
		handler := slogx.NewOtelHandler(baseHandler, nil)
		assert.True(t, handler.Enabled(context.Background(), slog.LevelDebug))
		assert.True(t, handler.Enabled(context.Background(), slog.LevelInfo))

		nilNextHandler := slogx.NewOtelHandler(nil, nil)
		assert.False(t, nilNextHandler.Enabled(context.Background(), slog.LevelInfo))
		assert.NoError(t, nilNextHandler.Handle(context.Background(), slog.Record{}))
	})
}
