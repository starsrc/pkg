package observability

import (
	"fmt"
	"net"
	"strings"

	"github.com/starsrc/pkg/observability/slogx"
	"github.com/starsrc/pkg/observability/telemetry"
)

// Ready 表示可观测性运行时（Logger、MeterProvider、TracerProvider）已完成初始化。
type Ready struct{}

// Config 定义统一的可观测性配置契约（聚合结构化日志、链路追踪、管理面路由开关与独立管理监听地址）。
type Config interface {
	slogx.Config
	telemetry.Config
	AdminRouteConfig
	// GetAdminAddr 返回独立管理面 HTTP 监听地址（如 "127.0.0.1:0"；为空或 "off" 表示不启动独立监听）。
	GetAdminAddr() string
}

// EnvConfig 聚合通过环境变量解析的全栈可观测性配置。
type EnvConfig struct {
	slogx.LogConfig
	telemetry.TelemetryConfig
	MetricsEnabled bool   `env:"METRICS_ENABLED" envDefault:"true"`
	PprofEnabled   bool   `env:"PPROF_ENABLED" envDefault:"true"`
	AdminAddr      string `env:"OBS_ADMIN_ADDR" envDefault:""`
}

// DefaultConfig 返回默认配置实例（JSON Info 日志、Metrics 与 Pprof 默认开启、OTel Tracing 默认关闭）。
func DefaultConfig() EnvConfig {
	return EnvConfig{
		LogConfig: slogx.LogConfig{
			LogLevel:          "info",
			LogFormat:         "json",
			LogConnectMessage: slogx.DefaultConnectMessage,
		},
		TelemetryConfig: telemetry.TelemetryConfig{
			OtelEnabled:        false,
			OtelServiceVersion: "0.1.0",
			DeployEnv:          "development",
			OtelTracesExporter: telemetry.ExporterOTLP,
			OtelEndpoint:       "localhost:4317",
			OtelInsecure:       true,
			OtelSampleRatio:    1.0,
			OtelTrustRemote:    false,
		},
		MetricsEnabled: true,
		PprofEnabled:   true,
		AdminAddr:      "",
	}
}

// IsMetricsEnabled 返回是否启用指标采集与导出。
func (c EnvConfig) IsMetricsEnabled() bool {
	return c.MetricsEnabled
}

// IsPprofEnabled 返回是否启用 pprof 剖析路由。
func (c EnvConfig) IsPprofEnabled() bool {
	return c.PprofEnabled
}

// GetAdminAddr 返回去除前后空白的管理监听地址。
func (c EnvConfig) GetAdminAddr() string {
	return strings.TrimSpace(c.AdminAddr)
}

// Validate 校验遥测参数与管理监听地址格式合法性。
func (c EnvConfig) Validate() error {
	if err := c.TelemetryConfig.Validate(); err != nil {
		return err
	}
	addr := c.GetAdminAddr()
	if addr != "" && !strings.EqualFold(addr, "off") {
		if _, _, err := net.SplitHostPort(addr); err != nil {
			return fmt.Errorf("invalid OBS_ADMIN_ADDR %q: %w", c.AdminAddr, err)
		}
	}
	return nil
}

// 编译期接口实现断言。
var (
	_ Config = EnvConfig{}
	_ Config = (*EnvConfig)(nil)
)
