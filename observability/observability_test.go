package observability_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/starsrc/pkg/observability"
	"github.com/starsrc/pkg/observability/slogx"
	"github.com/starsrc/pkg/observability/telemetry"
)

func TestPureCorePackages_ZeroFxDependency(t *testing.T) {
	// 认识论实证：直接通过 go list -deps 检验根包、slogx 与 telemetry 编译依赖树完全不含 go.uber.org/fx / dig / zap
	pkgs := []string{
		"github.com/starsrc/pkg/observability",
		"github.com/starsrc/pkg/observability/slogx",
		"github.com/starsrc/pkg/observability/telemetry",
	}
	cmd := exec.Command("go", append([]string{"list", "-deps"}, pkgs...)...)
	cmd.Env = append(cmd.Environ(), "GOWORK=off")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "go list -deps failed: %s", string(out))

	deps := string(out)
	assert.NotContains(t, deps, "go.uber.org/fx", "纯 Go 可观测性核心包严禁链入 go.uber.org/fx")
	assert.NotContains(t, deps, "go.uber.org/dig", "纯 Go 可观测性核心包严禁链入 go.uber.org/dig")
	assert.NotContains(t, deps, "go.uber.org/zap", "纯 Go 可观测性核心包严禁链入 go.uber.org/zap")
}

func TestDefaultServeMux_NotPollutedByPprof(t *testing.T) {
	// 验证引入 observability（内部含 pprof 处理器）绝未污染全局 http.DefaultServeMux
	req := httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil)
	rec := httptest.NewRecorder()
	http.DefaultServeMux.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code, "http.DefaultServeMux 上严禁自动暴露 /debug/pprof/")
}

func TestSetup_AdminAddr_OpenMetricsExemplars_Pprof_HealthAndReadyz(t *testing.T) {
	ctx := context.Background()

	origTP := otel.GetTracerProvider()
	origMP := otel.GetMeterProvider()
	origProp := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(origTP)
		otel.SetMeterProvider(origMP)
		otel.SetTextMapPropagator(origProp)
	})

	customGauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "bowl_custom_fuse_inflight",
		Help: "Custom collector registered into isolated registry.",
	})
	customGauge.Set(42)

	var logBuf bytes.Buffer
	cfg := observability.DefaultConfig()
	cfg.OtelEnabled = true
	cfg.OtelServiceName = "bowl-daemon"
	cfg.OtelTracesExporter = telemetry.ExporterNone
	cfg.OtelSampleRatio = 1.0
	cfg.MetricsEnabled = true
	cfg.PprofEnabled = true
	cfg.AdminAddr = "127.0.0.1:0"

	rt, err := observability.Setup(
		ctx,
		cfg,
		observability.WithLogWriter(&logBuf),
		observability.WithSetDefaultLogger(false),
		observability.WithCollectors(customGauge),
		observability.WithReadyCheck("storage_engine", func(ctx context.Context) error {
			return nil
		}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rt.Shutdown(context.Background()) })

	adminAddr := rt.AdminAddr()
	require.NotEmpty(t, adminAddr)
	assert.NotEqual(t, "127.0.0.1:0", adminAddr, "AdminAddr() 必须返回内核分配的真实监听地址")

	// 在活跃已采样 Span 上记录 OTel Histogram，实证 OpenMetrics 1.0 输出 Trace Exemplar
	spanCtx, span := otel.Tracer("test").Start(ctx, "op.write")
	hist, err := otel.Meter("test").Float64Histogram("rpc.test.latency")
	require.NoError(t, err)
	hist.Record(spanCtx, 0.012, metric.WithAttributes(attribute.String("op", "write")))
	span.End()

	rt.Logger().InfoContext(spanCtx, "write finished")
	assert.Contains(t, logBuf.String(), span.SpanContext().TraceID().String(), "日志应自动携带同一个 trace_id")

	baseURL := "http://" + adminAddr
	client := &http.Client{Timeout: 3 * time.Second}

	// 1. 验证 /metrics OpenMetrics 格式与 Trace Exemplar + 自定义 Collector
	reqMetrics, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/metrics", nil)
	require.NoError(t, err)
	reqMetrics.Header.Set("Accept", "application/openmetrics-text;version=1.0.0;q=0.9,*/*;q=0.1")
	respMetrics, err := client.Do(reqMetrics)
	require.NoError(t, err)
	metricsBody, err := io.ReadAll(respMetrics.Body)
	_ = respMetrics.Body.Close()
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, respMetrics.StatusCode)
	metricsText := string(metricsBody)
	assert.Contains(t, metricsText, "bowl_custom_fuse_inflight 42")
	assert.Contains(t, metricsText, "rpc_test_latency")
	assert.Contains(t, metricsText, `trace_id="`+span.SpanContext().TraceID().String()+`"`, "OpenMetrics 响应应包含活跃 Span 的 Trace Exemplar")

	// 2. 验证 /debug/pprof/ 与 /debug/pprof/cmdline
	respPprof, err := client.Get(baseURL + "/debug/pprof/")
	require.NoError(t, err)
	_ = respPprof.Body.Close()
	assert.Equal(t, http.StatusOK, respPprof.StatusCode)

	respCmdline, err := client.Get(baseURL + "/debug/pprof/cmdline")
	require.NoError(t, err)
	_ = respCmdline.Body.Close()
	assert.Equal(t, http.StatusOK, respCmdline.StatusCode)

	// 3. 验证 /healthz 与 /readyz 200 JSON
	respHealth, err := client.Get(baseURL + "/healthz")
	require.NoError(t, err)
	var healthBody map[string]any
	require.NoError(t, json.NewDecoder(respHealth.Body).Decode(&healthBody))
	_ = respHealth.Body.Close()
	assert.Equal(t, http.StatusOK, respHealth.StatusCode)
	assert.Equal(t, "ok", healthBody["status"])

	respReady, err := client.Get(baseURL + "/readyz")
	require.NoError(t, err)
	var readyBody map[string]any
	require.NoError(t, json.NewDecoder(respReady.Body).Decode(&readyBody))
	_ = respReady.Body.Close()
	assert.Equal(t, http.StatusOK, respReady.StatusCode)
	assert.Equal(t, "ok", readyBody["status"])

	// 4. 验证优雅停机后监听端口释放
	require.NoError(t, rt.Shutdown(ctx))
	_, errAfterStop := client.Get(baseURL + "/healthz")
	require.Error(t, errAfterStop, "Shutdown 后独立 Admin HTTP 端口应已关闭")
}

func TestRegisterAdminRoutes_ReadyzFailureAndTimeout(t *testing.T) {
	t.Run("ReadyCheck 返回错误时 /readyz 返回 503 JSON 而 /healthz 保持 200", func(t *testing.T) {
		mux := http.NewServeMux()
		observability.RegisterAdminRoutes(
			mux,
			nil,
			observability.AdminConfig{MetricsEnabled: false, PprofEnabled: false},
			observability.WithReadyCheck("postgres", func(ctx context.Context) error {
				return errors.New("connection refused")
			}),
		)

		recReady := httptest.NewRecorder()
		mux.ServeHTTP(recReady, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		assert.Equal(t, http.StatusServiceUnavailable, recReady.Code)

		var readyPayload struct {
			Status string            `json:"status"`
			Checks map[string]string `json:"checks"`
		}
		require.NoError(t, json.Unmarshal(recReady.Body.Bytes(), &readyPayload))
		assert.Equal(t, "unavailable", readyPayload.Status)
		assert.Equal(t, "connection refused", readyPayload.Checks["postgres"])

		recHealth := httptest.NewRecorder()
		mux.ServeHTTP(recHealth, httptest.NewRequest(http.MethodGet, "/healthz", nil))
		assert.Equal(t, http.StatusOK, recHealth.Code)

		// MetricsEnabled=false 且 PprofEnabled=false 时相关路由应返回 404
		recMetrics := httptest.NewRecorder()
		mux.ServeHTTP(recMetrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		assert.Equal(t, http.StatusNotFound, recMetrics.Code)

		recPprof := httptest.NewRecorder()
		mux.ServeHTTP(recPprof, httptest.NewRequest(http.MethodGet, "/debug/pprof/", nil))
		assert.Equal(t, http.StatusNotFound, recPprof.Code)
	})

	t.Run("ReadyCheck 超时阻塞时受超时上限保护返回 503", func(t *testing.T) {
		mux := http.NewServeMux()
		observability.RegisterAdminRoutes(
			mux,
			nil,
			observability.AdminConfig{},
			observability.WithReadyTimeout(40*time.Millisecond),
			observability.WithReadyCheck("slow_db", func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			}),
		)

		start := time.Now()
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		elapsed := time.Since(start)

		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
		assert.Less(t, elapsed, 500*time.Millisecond)
		assert.True(t, strings.Contains(rec.Body.String(), "context deadline exceeded"))
	})
}

func TestEnvConfig_Validate(t *testing.T) {
	cfg := observability.EnvConfig{
		LogConfig:       slogx.LogConfig{LogLevel: "info"},
		TelemetryConfig: telemetry.TelemetryConfig{OtelSampleRatio: 1.0, OtelTracesExporter: "otlp"},
		AdminAddr:       "invalid-addr-without-port",
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid OBS_ADMIN_ADDR")

	cfg.AdminAddr = "off"
	require.NoError(t, cfg.Validate())

	cfg.AdminAddr = "127.0.0.1:0"
	require.NoError(t, cfg.Validate())
}

// 确保即使传入最小 sdktrace 依赖也能编译通过
var _ *sdktrace.TracerProvider
