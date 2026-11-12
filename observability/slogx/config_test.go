package slogx_test

import (
	"log/slog"
	"testing"

	"github.com/caarlos0/env/v11"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/pkg/observability/slogx"
)

var (
	_ slogx.Config = slogx.EnvConfig{}
	_ slogx.Config = (*slogx.EnvConfig)(nil)
	_ slogx.Config = slogx.LogConfig{}
	_ slogx.Config = (*slogx.LogConfig)(nil)
)

func TestEnvConfig_Defaults(t *testing.T) {
	for _, envKey := range []string{
		"LOG_LEVEL",
		"LOG_FORMAT",
		"LOG_ADD_SOURCE",
		"OTEL_SERVICE_NAME",
		"APP_ENV",
		"APP_VERSION",
		"LOG_CONNECT_MESSAGE",
		"LOG_OTEL_RECORD_TRACE_FLAGS",
		"LOG_OTEL_SAMPLED_ONLY",
	} {
		t.Setenv(envKey, "")
	}

	var cfg slogx.EnvConfig
	err := env.Parse(&cfg)
	require.NoError(t, err)

	assert.Equal(t, "info", cfg.Level())
	assert.Equal(t, "json", cfg.Format())
	assert.Equal(t, slog.LevelInfo, cfg.SlogLevel())
	assert.Equal(t, "handled rpc", cfg.Message())
	assert.False(t, cfg.RecordTraceFlags())
	assert.False(t, cfg.SampledOnly())
}

func TestEnvConfig_Message_Fallback(t *testing.T) {
	cfg := slogx.EnvConfig{LogConnectMessage: ""}
	assert.Equal(t, "handled rpc", cfg.Message())
}

func TestEnvConfig_EnvParse(t *testing.T) {
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("LOG_FORMAT", "text")
	t.Setenv("LOG_CONNECT_MESSAGE", "custom rpc complete")
	t.Setenv("LOG_OTEL_RECORD_TRACE_FLAGS", "true")
	t.Setenv("LOG_OTEL_SAMPLED_ONLY", "true")

	var cfg slogx.EnvConfig
	err := env.Parse(&cfg)
	require.NoError(t, err)

	assert.Equal(t, "debug", cfg.Level())
	assert.Equal(t, "text", cfg.Format())
	assert.Equal(t, slog.LevelDebug, cfg.SlogLevel())
	assert.Equal(t, "custom rpc complete", cfg.Message())
	assert.True(t, cfg.RecordTraceFlags())
	assert.True(t, cfg.SampledOnly())
}

func TestEnvConfig_SlogLevel_Mapping(t *testing.T) {
	tests := []struct {
		name     string
		levelStr string
		expected slog.Level
	}{
		{name: "debug lowercase", levelStr: "debug", expected: slog.LevelDebug},
		{name: "debug uppercase", levelStr: "DEBUG", expected: slog.LevelDebug},
		{name: "debug with whitespace", levelStr: "  debug  ", expected: slog.LevelDebug},
		{name: "info lowercase", levelStr: "info", expected: slog.LevelInfo},
		{name: "info uppercase", levelStr: "INFO", expected: slog.LevelInfo},
		{name: "warn lowercase", levelStr: "warn", expected: slog.LevelWarn},
		{name: "warning lowercase", levelStr: "warning", expected: slog.LevelWarn},
		{name: "warn uppercase", levelStr: "WARN", expected: slog.LevelWarn},
		{name: "warning uppercase", levelStr: "WARNING", expected: slog.LevelWarn},
		{name: "warning mixed case", levelStr: "Warning", expected: slog.LevelWarn},
		{name: "error lowercase", levelStr: "error", expected: slog.LevelError},
		{name: "error uppercase", levelStr: "ERROR", expected: slog.LevelError},
		{name: "empty string fallback to info", levelStr: "", expected: slog.LevelInfo},
		{name: "unknown fallback to info", levelStr: "unknown", expected: slog.LevelInfo},
		{name: "invalid fallback to info", levelStr: "fatal", expected: slog.LevelInfo},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := slogx.EnvConfig{LogLevel: tc.levelStr}
			assert.Equal(t, tc.expected, cfg.SlogLevel())
		})
	}
}
