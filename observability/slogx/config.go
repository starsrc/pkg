package slogx

import (
	"log/slog"
	"strings"
)

// EnvConfig 承载通过环境变量解析的统一结构化日志与访问日志配置。
type EnvConfig struct {
	LogLevel                string `env:"LOG_LEVEL" envDefault:"info"`
	LogFormat               string `env:"LOG_FORMAT" envDefault:"json"`
	LogAddSource            bool   `env:"LOG_ADD_SOURCE" envDefault:"false"`
	LogConnectMessage       string `env:"LOG_CONNECT_MESSAGE" envDefault:"handled rpc"`
	LogOtelRecordTraceFlags bool   `env:"LOG_OTEL_RECORD_TRACE_FLAGS" envDefault:"false"`
	LogOtelSampledOnly      bool   `env:"LOG_OTEL_SAMPLED_ONLY" envDefault:"false"`
}

// LogConfig 是 EnvConfig 的类型别名，方便下游微服务按领域习惯嵌入。
type LogConfig = EnvConfig

// Level 返回配置的原始日志级别字符串。
func (c EnvConfig) Level() string {
	return c.LogLevel
}

// Format 返回日志输出格式（如 "json", "text"）。
func (c EnvConfig) Format() string {
	return c.LogFormat
}

// AddSource 返回是否在日志记录中附加调用源码文件名与行号。
func (c EnvConfig) AddSource() bool {
	return c.LogAddSource
}

// SlogLevel 将配置的日志级别字符串转换为 slog.Level。
// 支持大小写不敏感匹配 debug、info、warn、warning、error，未知级别回退至 slog.LevelInfo。
func (c EnvConfig) SlogLevel() slog.Level {
	switch strings.ToLower(strings.TrimSpace(c.LogLevel)) {
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

// Message 返回 ConnectRPC 请求完成时的访问日志消息文本。
func (c EnvConfig) Message() string {
	if c.LogConnectMessage == "" {
		return "handled rpc"
	}
	return c.LogConnectMessage
}

// RecordTraceFlags 返回是否在日志中附加 TraceFlags 属性。
func (c EnvConfig) RecordTraceFlags() bool {
	return c.LogOtelRecordTraceFlags
}

// SampledOnly 返回是否仅对已采样的 Span 注入追踪属性。
func (c EnvConfig) SampledOnly() bool {
	return c.LogOtelSampledOnly
}

// 编译期接口实现断言。
var (
	_ Config = EnvConfig{}
	_ Config = (*EnvConfig)(nil)
)
