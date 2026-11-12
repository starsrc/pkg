package telemetry_test

import (
	"math"
	"os"
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/pkg/observability/telemetry"
)

var (
	_ telemetry.Config = telemetry.EnvConfig{}
	_ telemetry.Config = (*telemetry.EnvConfig)(nil)
	_ telemetry.Config = telemetry.TelemetryConfig{}
	_ telemetry.Config = (*telemetry.TelemetryConfig)(nil)
)

func TestEnvConfig_Defaults(t *testing.T) {
	for _, key := range []string{
		"OTEL_ENABLED",
		"OTEL_SERVICE_NAME",
		"OTEL_SERVICE_VERSION",
		"DEPLOY_ENV",
		"OTEL_TRACES_EXPORTER",
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
		"OTEL_EXPORTER_OTLP_INSECURE",
		"OTEL_EXPORTER_OTLP_TRACES_INSECURE",
		"OTEL_TRACES_SAMPLER_ARG",
		"OTEL_TRUST_REMOTE",
	} {
		oldVal, exists := os.LookupEnv(key)
		os.Unsetenv(key)
		k := key
		if exists {
			v := oldVal
			t.Cleanup(func() { _ = os.Setenv(k, v) })
		} else {
			t.Cleanup(func() { _ = os.Unsetenv(k) })
		}
	}

	var cfg telemetry.EnvConfig
	err := env.Parse(&cfg)
	require.NoError(t, err)

	assert.False(t, cfg.Enabled())
	assert.False(t, cfg.TelemetryEnabled())
	assert.Equal(t, "", cfg.ServiceName())
	assert.Equal(t, "0.1.0", cfg.ServiceVersion())
	assert.Equal(t, "development", cfg.DeploymentEnvironment())
	assert.Equal(t, "otlp", cfg.TracesExporter())
	assert.Equal(t, "localhost:4317", cfg.Endpoint())
	assert.Equal(t, "localhost:4317", cfg.OtlpEndpoint())
	assert.True(t, cfg.Insecure())
	assert.True(t, cfg.OtlpInsecure())
	assert.InDelta(t, 1.0, cfg.SampleRatio(), 0.0001)
	assert.False(t, cfg.TrustRemote())
}

func TestEnvConfig_EnvParse(t *testing.T) {
	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_SERVICE_NAME", "my-service")
	t.Setenv("OTEL_SERVICE_VERSION", "1.2.3")
	t.Setenv("DEPLOY_ENV", "production")
	t.Setenv("OTEL_TRACES_EXPORTER", "none")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "base-collector:4317")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "trace-collector:4317")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "false")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_INSECURE", "true")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "0.25")
	t.Setenv("OTEL_TRUST_REMOTE", "true")

	var cfg telemetry.EnvConfig
	err := env.Parse(&cfg)
	require.NoError(t, err)

	assert.True(t, cfg.Enabled())
	assert.True(t, cfg.TelemetryEnabled())
	assert.Equal(t, "my-service", cfg.ServiceName())
	assert.Equal(t, "1.2.3", cfg.ServiceVersion())
	assert.Equal(t, "production", cfg.DeploymentEnvironment())
	assert.Equal(t, "none", cfg.TracesExporter())
	assert.Equal(t, "trace-collector:4317", cfg.Endpoint())
	assert.Equal(t, "trace-collector:4317", cfg.OtlpEndpoint())
	assert.True(t, cfg.Insecure())
	assert.True(t, cfg.OtlpInsecure())
	assert.InDelta(t, 0.25, cfg.SampleRatio(), 0.0001)
	assert.True(t, cfg.TrustRemote())
}

func TestEnvConfig_ApplyDefaultTrustRemote(t *testing.T) {
	t.Run("环境变量未设置时 ApplyDefaultTrustRemote 生效", func(t *testing.T) {
		oldVal, exists := os.LookupEnv("OTEL_TRUST_REMOTE")
		os.Unsetenv("OTEL_TRUST_REMOTE")
		t.Cleanup(func() {
			if exists {
				_ = os.Setenv("OTEL_TRUST_REMOTE", oldVal)
			} else {
				_ = os.Unsetenv("OTEL_TRUST_REMOTE")
			}
		})

		var cfg telemetry.EnvConfig
		require.NoError(t, env.Parse(&cfg))
		assert.False(t, cfg.TrustRemote())

		cfg.ApplyDefaultTrustRemote(true)
		assert.True(t, cfg.TrustRemote())
	})

	t.Run("环境变量显式设置为 false 时 ApplyDefaultTrustRemote(true) 不覆盖", func(t *testing.T) {
		t.Setenv("OTEL_TRUST_REMOTE", "false")
		var cfg telemetry.EnvConfig
		require.NoError(t, env.Parse(&cfg))

		cfg.ApplyDefaultTrustRemote(true)
		assert.False(t, cfg.TrustRemote(), "显式环境变量 OTEL_TRUST_REMOTE=false 不得被默认策略覆盖")
	})

	t.Run("nil 接收者安全返回", func(t *testing.T) {
		var cfg *telemetry.EnvConfig
		assert.NotPanics(t, func() {
			cfg.ApplyDefaultTrustRemote(true)
		})
	})
}

func TestEnvConfig_Endpoint_Priority_And_Trim(t *testing.T) {
	t.Run("专属链路端点非空时优先使用且修剪空白", func(t *testing.T) {
		cfg := telemetry.EnvConfig{
			OtelEndpoint:       " base-collector:4317 ",
			OtelTracesEndpoint: " trace-collector:4317 ",
		}
		assert.Equal(t, "trace-collector:4317", cfg.Endpoint())
		assert.Equal(t, "trace-collector:4317", cfg.OtlpEndpoint())
	})

	t.Run("专属链路端点为空或纯空白时回退至基础端点并修剪空白", func(t *testing.T) {
		cfg := telemetry.EnvConfig{
			OtelEndpoint:       " base-collector:4317 ",
			OtelTracesEndpoint: "   ",
		}
		assert.Equal(t, "base-collector:4317", cfg.Endpoint())
		assert.Equal(t, "base-collector:4317", cfg.OtlpEndpoint())
	})
}

func TestEnvConfig_DefaultServiceName(t *testing.T) {
	t.Run("ApplyDefaultServiceName 在服务名为空时设置默认值", func(t *testing.T) {
		cfg := telemetry.EnvConfig{OtelServiceName: ""}
		cfg.ApplyDefaultServiceName("aether-auth")
		assert.Equal(t, "aether-auth", cfg.ServiceName())
	})

	t.Run("WithDefaultServiceName 链式设置默认服务名", func(t *testing.T) {
		cfg := telemetry.EnvConfig{OtelServiceName: ""}.WithDefaultServiceName("aether-auth")
		assert.Equal(t, "aether-auth", cfg.ServiceName())

		cfgExisting := telemetry.EnvConfig{OtelServiceName: "custom"}.WithDefaultServiceName("aether-auth")
		assert.Equal(t, "custom", cfgExisting.ServiceName())
	})
}

func TestEnvConfig_Validate(t *testing.T) {
	tests := []struct {
		name        string
		cfg         telemetry.EnvConfig
		wantErr     bool
		errContains string
	}{
		{name: "合法采样率 0.0 + otlp", cfg: telemetry.EnvConfig{OtelSampleRatio: 0.0, OtelTracesExporter: "otlp"}, wantErr: false},
		{name: "合法采样率 0.5 + none", cfg: telemetry.EnvConfig{OtelSampleRatio: 0.5, OtelTracesExporter: "none"}, wantErr: false},
		{name: "合法采样率 1.0 + 空导出器默认otlp", cfg: telemetry.EnvConfig{OtelSampleRatio: 1.0}, wantErr: false},
		{name: "非法负数采样率 -0.1", cfg: telemetry.EnvConfig{OtelSampleRatio: -0.1}, wantErr: true, errContains: "invalid OTEL_TRACES_SAMPLER_ARG -0.1"},
		{name: "非法超限采样率 1.5", cfg: telemetry.EnvConfig{OtelSampleRatio: 1.5}, wantErr: true, errContains: "invalid OTEL_TRACES_SAMPLER_ARG 1.5"},
		{name: "非法 NaN 采样率", cfg: telemetry.EnvConfig{OtelSampleRatio: math.NaN()}, wantErr: true, errContains: "invalid OTEL_TRACES_SAMPLER_ARG NaN"},
		{name: "非法 +Inf 采样率", cfg: telemetry.EnvConfig{OtelSampleRatio: math.Inf(1)}, wantErr: true, errContains: "invalid OTEL_TRACES_SAMPLER_ARG +Inf"},
		{name: "非法导出器类型", cfg: telemetry.EnvConfig{OtelSampleRatio: 1.0, OtelTracesExporter: "zipkin"}, wantErr: true, errContains: "invalid OTEL_TRACES_EXPORTER"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
