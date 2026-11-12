package slogx

import "log/slog"

// Config 定义统一的结构化日志、链路追踪注入及 RPC 访问日志配置契约。
type Config interface {
	// SlogLevel 提供日志输出级别（如 slog.LevelInfo, slog.LevelDebug 等）。
	SlogLevel() slog.Level
	// Format 提供日志输出格式（如 "json", "text"）。
	Format() string
	// Message 提供 ConnectRPC 请求完成时的访问日志消息文本。
	Message() string
	// RecordTraceFlags 判定是否在日志中附加 TraceFlags 属性。
	RecordTraceFlags() bool
	// SampledOnly 判定是否仅对已采样的 Span 注入追踪属性。
	SampledOnly() bool
}
