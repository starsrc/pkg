package slogx

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

type loggerOptions struct {
	writer     io.Writer
	setDefault bool
}

// Option 定义构造 *slog.Logger 的函数式选项。
type Option func(*loggerOptions)

// WithWriter 指定日志底层输出目标（如 CLI 使用 os.Stderr 或守护进程写入日志文件）；传入 nil 时忽略并回退至 os.Stdout。
func WithWriter(w io.Writer) Option {
	return func(o *loggerOptions) {
		if w != nil {
			o.writer = w
		}
	}
}

// WithSetDefault 控制是否将新建的 *slog.Logger 注册为全局 slog.Default()（默认 true）。
func WithSetDefault(set bool) Option {
	return func(o *loggerOptions) {
		o.setDefault = set
	}
}

// New 根据统一日志配置与可选参数创建 *slog.Logger。
// 默认写入 os.Stdout 并设为全局 slog.Default()，可通过 WithWriter 与 WithSetDefault 覆盖。
func New(cfg Config, opts ...Option) *slog.Logger {
	options := loggerOptions{
		writer:     os.Stdout,
		setDefault: true,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}

	level := slog.LevelInfo
	format := "json"
	var addSource bool
	if cfg != nil {
		level = cfg.SlogLevel()
		format = cfg.Format()
		if sc, ok := cfg.(interface{ AddSource() bool }); ok {
			addSource = sc.AddSource()
		}
	}

	handlerOpts := &slog.HandlerOptions{
		Level:     level,
		AddSource: addSource,
	}
	var baseHandler slog.Handler
	if strings.EqualFold(format, "text") {
		baseHandler = slog.NewTextHandler(options.writer, handlerOpts)
	} else {
		baseHandler = slog.NewJSONHandler(options.writer, handlerOpts)
	}
	otelHandler := NewOtelHandler(baseHandler, cfg)
	logger := slog.New(otelHandler)
	if options.setDefault {
		slog.SetDefault(logger)
	}
	return logger
}

const (
	// DefaultTraceIDKey 是默认的 TraceID 日志属性键。
	DefaultTraceIDKey = "trace_id"
	// DefaultSpanIDKey 是默认的 SpanID 日志属性键。
	DefaultSpanIDKey = "span_id"
	// DefaultTraceFlagsKey 是默认的 TraceFlags 日志属性键。
	DefaultTraceFlagsKey = "trace_flags"
)

// OtelHandler 包装底层的 slog.Handler，自动从 context.Context 提取 OTel Trace 信息并注入日志。
type OtelHandler struct {
	next             slog.Handler
	recordTraceFlags bool
	sampledOnly      bool
}

var _ slog.Handler = (*OtelHandler)(nil)

// NewOtelHandler 创建一个新的 slogx.OtelHandler 装饰器。
func NewOtelHandler(next slog.Handler, cfg Config) *OtelHandler {
	var recordTraceFlags, sampledOnly bool
	if cfg != nil {
		recordTraceFlags = cfg.RecordTraceFlags()
		sampledOnly = cfg.SampledOnly()
	}
	return &OtelHandler{
		next:             next,
		recordTraceFlags: recordTraceFlags,
		sampledOnly:      sampledOnly,
	}
}

// Handler 返回被包装的底层 slog.Handler。
func (h *OtelHandler) Handler() slog.Handler {
	return h.next
}

// Unwrap 返回被包装的底层 slog.Handler。
func (h *OtelHandler) Unwrap() slog.Handler {
	return h.next
}

// Enabled 委托底层 Handler 判定指定日志级别是否启用。
func (h *OtelHandler) Enabled(ctx context.Context, level slog.Level) bool {
	if h.next == nil {
		return false
	}
	return h.next.Enabled(ctx, level)
}

// Handle 提取 context 中的 OTel Span 追踪上下文并注入属性，随后委托底层 Handler 输出。
func (h *OtelHandler) Handle(ctx context.Context, record slog.Record) error {
	if h.next == nil {
		return nil
	}
	span := trace.SpanFromContext(ctx)
	sc := span.SpanContext()

	if !sc.IsValid() || (h.sampledOnly && !sc.IsSampled()) {
		return h.next.Handle(ctx, record)
	}

	attrs := make([]slog.Attr, 0, 3)
	attrs = append(attrs, slog.String(DefaultTraceIDKey, sc.TraceID().String()))
	attrs = append(attrs, slog.String(DefaultSpanIDKey, sc.SpanID().String()))
	if h.recordTraceFlags {
		attrs = append(attrs, slog.String(DefaultTraceFlagsKey, sc.TraceFlags().String()))
	}
	record.AddAttrs(attrs...)

	return h.next.Handle(ctx, record)
}

// WithAttrs 返回附加了属性的新 Handler，保持追踪装饰能力。
func (h *OtelHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	var next slog.Handler
	if h.next != nil {
		next = h.next.WithAttrs(attrs)
	}
	return &OtelHandler{
		next:             next,
		recordTraceFlags: h.recordTraceFlags,
		sampledOnly:      h.sampledOnly,
	}
}

// WithGroup 返回创建了属性分组的新 Handler，保持追踪装饰能力。
func (h *OtelHandler) WithGroup(name string) slog.Handler {
	var next slog.Handler
	if h.next != nil {
		next = h.next.WithGroup(name)
	}
	return &OtelHandler{
		next:             next,
		recordTraceFlags: h.recordTraceFlags,
		sampledOnly:      h.sampledOnly,
	}
}
