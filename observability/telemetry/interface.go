package telemetry

// Config 定义跨微服务与客户端通用的 OpenTelemetry 遥测与链路追踪配置契约。
type Config interface {
	// Enabled 返回是否开启链路追踪。
	Enabled() bool
	// ServiceName 返回遥测服务名称。
	ServiceName() string
	// ServiceVersion 返回遥测服务版本。
	ServiceVersion() string
	// DeploymentEnvironment 返回部署环境名称（如 development、staging、production）。
	DeploymentEnvironment() string
	// TracesExporter 返回链路追踪导出器类型（"otlp" 或 "none"）。
	TracesExporter() string
	// Endpoint 返回 OTLP 收集端接入地址（如 jaeger:4317 或 http(s) URL）。
	Endpoint() string
	// Insecure 返回是否使用非安全传输协议（跳过 TLS）。
	Insecure() bool
	// SampleRatio 返回链路采样率（0.0 至 1.0 之间）。
	SampleRatio() float64
	// TrustRemote 返回是否信任上游传播的链路追踪上下文。
	TrustRemote() bool
}
