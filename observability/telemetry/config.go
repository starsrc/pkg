package telemetry

import (
	"fmt"
	"math"
	"os"
	"strings"
)

const (
	// ExporterOTLP 通过 OTLP gRPC 将采样 Span 异步批量导出至 Collector。
	ExporterOTLP = "otlp"
	// ExporterNone 仅启用 W3C TraceContext/Baggage 传播与 TraceID/SpanID 生成，不启动后台 OTLP 导出协程。
	ExporterNone = "none"
)

// EnvConfig 承载通过环境变量解析的 OpenTelemetry 遥测与链路追踪配置。
type EnvConfig struct {
	OtelEnabled        bool    `env:"OTEL_ENABLED" envDefault:"false"`
	OtelServiceName    string  `env:"OTEL_SERVICE_NAME"`
	OtelServiceVersion string  `env:"OTEL_SERVICE_VERSION" envDefault:"0.1.0"`
	DeployEnv          string  `env:"DEPLOY_ENV" envDefault:"development"`
	OtelTracesExporter string  `env:"OTEL_TRACES_EXPORTER" envDefault:"otlp"`
	OtelEndpoint       string  `env:"OTEL_EXPORTER_OTLP_ENDPOINT" envDefault:"localhost:4317"`
	OtelTracesEndpoint string  `env:"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"`
	OtelInsecure       bool    `env:"OTEL_EXPORTER_OTLP_INSECURE" envDefault:"true"`
	OtelTracesInsecure *bool   `env:"OTEL_EXPORTER_OTLP_TRACES_INSECURE"`
	OtelSampleRatio    float64 `env:"OTEL_TRACES_SAMPLER_ARG" envDefault:"1.0"`
	OtelTrustRemote    bool    `env:"OTEL_TRUST_REMOTE" envDefault:"false"`
}

// TelemetryConfig 是 EnvConfig 的类型别名，方便下游按领域习惯嵌入。
type TelemetryConfig = EnvConfig

// Enabled 返回是否开启链路追踪。
func (c EnvConfig) Enabled() bool {
	return c.OtelEnabled
}

// TelemetryEnabled 语义等价于 Enabled，便于下游结构体按显式命名调用。
func (c EnvConfig) TelemetryEnabled() bool {
	return c.Enabled()
}

// ServiceName 返回去除前后空白的遥测服务名称。
func (c EnvConfig) ServiceName() string {
	return strings.TrimSpace(c.OtelServiceName)
}

// ServiceVersion 返回去除前后空白的遥测服务版本。
func (c EnvConfig) ServiceVersion() string {
	return strings.TrimSpace(c.OtelServiceVersion)
}

// DeploymentEnvironment 返回去除前后空白的部署环境名称。
func (c EnvConfig) DeploymentEnvironment() string {
	return strings.TrimSpace(c.DeployEnv)
}

// TracesExporter 返回归一化（小写去空白）的链路导出器类型，空值默认回退至 "otlp"。
func (c EnvConfig) TracesExporter() string {
	v := strings.ToLower(strings.TrimSpace(c.OtelTracesExporter))
	if v == "" {
		return ExporterOTLP
	}
	return v
}

// Endpoint 返回 OTLP 收集端接入地址（优先 OTEL_EXPORTER_OTLP_TRACES_ENDPOINT，其次 OTEL_EXPORTER_OTLP_ENDPOINT 并修剪前后空白）。
func (c EnvConfig) Endpoint() string {
	if tracesEndpoint := strings.TrimSpace(c.OtelTracesEndpoint); tracesEndpoint != "" {
		return tracesEndpoint
	}
	return strings.TrimSpace(c.OtelEndpoint)
}

// OtlpEndpoint 语义等价于 Endpoint，便于下游结构体按显式命名调用。
func (c EnvConfig) OtlpEndpoint() string {
	return c.Endpoint()
}

// Insecure 返回是否使用非安全传输协议（优先 OTEL_EXPORTER_OTLP_TRACES_INSECURE，其次 OTEL_EXPORTER_OTLP_INSECURE）。
func (c EnvConfig) Insecure() bool {
	if c.OtelTracesInsecure != nil {
		return *c.OtelTracesInsecure
	}
	return c.OtelInsecure
}

// OtlpInsecure 语义等价于 Insecure，便于下游结构体按显式命名调用。
func (c EnvConfig) OtlpInsecure() bool {
	return c.Insecure()
}

// SampleRatio 返回链路采样率（0.0 至 1.0 之间）。
func (c EnvConfig) SampleRatio() float64 {
	return c.OtelSampleRatio
}

// TrustRemote 返回是否信任上游传播的链路追踪上下文。
func (c EnvConfig) TrustRemote() bool {
	return c.OtelTrustRemote
}

// ApplyDefaultServiceName 在当前服务名为空或纯空白时写入默认服务名称。
func (c *EnvConfig) ApplyDefaultServiceName(defaultName string) {
	if c == nil {
		return
	}
	if strings.TrimSpace(c.OtelServiceName) == "" {
		c.OtelServiceName = defaultName
	}
}

// WithDefaultServiceName 返回包含默认服务名兜底的新配置副本。
func (c EnvConfig) WithDefaultServiceName(defaultName string) EnvConfig {
	c.ApplyDefaultServiceName(defaultName)
	return c
}

// ApplyDefaultTrustRemote 仅当环境变量 OTEL_TRUST_REMOTE 未显式设置（或为空白）时覆盖默认信任策略。
func (c *EnvConfig) ApplyDefaultTrustRemote(trust bool) {
	if c == nil {
		return
	}
	if envVal, envSet := os.LookupEnv("OTEL_TRUST_REMOTE"); !envSet || strings.TrimSpace(envVal) == "" {
		c.OtelTrustRemote = trust
	}
}

// WithDefaultTrustRemote 返回应用了默认远端追踪信任策略的新配置副本。
func (c EnvConfig) WithDefaultTrustRemote(trust bool) EnvConfig {
	c.ApplyDefaultTrustRemote(trust)
	return c
}

// Validate 校验链路追踪采样率与导出器类型合法性。
func (c EnvConfig) Validate() error {
	if math.IsNaN(c.OtelSampleRatio) || math.IsInf(c.OtelSampleRatio, 0) || c.OtelSampleRatio < 0 || c.OtelSampleRatio > 1 {
		return fmt.Errorf("invalid OTEL_TRACES_SAMPLER_ARG %v: must be between 0.0 and 1.0", c.OtelSampleRatio)
	}
	switch c.TracesExporter() {
	case ExporterOTLP, ExporterNone:
	default:
		return fmt.Errorf("invalid OTEL_TRACES_EXPORTER %q: must be %q or %q", c.OtelTracesExporter, ExporterOTLP, ExporterNone)
	}
	return nil
}

// 编译期接口实现断言。
var (
	_ Config = EnvConfig{}
	_ Config = (*EnvConfig)(nil)
)
