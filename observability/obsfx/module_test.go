package obsfx_test

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.uber.org/fx"

	"github.com/starsrc/pkg/observability"
	"github.com/starsrc/pkg/observability/obsfx"
	"github.com/starsrc/pkg/observability/telemetry"
)

func TestModule_ProvidesRuntimeAndNeverInjectsConnectInterceptors(t *testing.T) {
	origTP := otel.GetTracerProvider()
	origMP := otel.GetMeterProvider()
	origProp := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(origTP)
		otel.SetMeterProvider(origMP)
		otel.SetTextMapPropagator(origProp)
	})

	ctx := context.Background()
	cfg := observability.DefaultConfig()
	cfg.OtelEnabled = true
	cfg.OtelServiceName = "aether-fx-test"
	cfg.OtelTracesExporter = telemetry.ExporterNone

	customCounter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "obsfx_test_events_total",
		Help: "Test counter injected via obsfx.AsCollector.",
	})
	customCounter.Inc()

	var logBuf bytes.Buffer
	var (
		rt       *observability.Runtime
		logger   *slog.Logger
		reg      *prometheus.Registry
		gatherer prometheus.Gatherer
		ready    observability.Ready
	)

	app := fx.New(
		fx.Supply(fx.Annotate(cfg, fx.As(new(observability.Config)))),
		fx.Provide(
			obsfx.AsOption(func() observability.Option {
				return observability.WithLogWriter(&logBuf)
			}),
			obsfx.AsOption(func() observability.Option {
				return observability.WithSetDefaultLogger(false)
			}),
			obsfx.AsCollector(func() prometheus.Collector {
				return customCounter
			}),
		),
		obsfx.Module,
		fx.Populate(&rt, &logger, &reg, &gatherer, &ready),
		fx.NopLogger,
	)

	require.NoError(t, app.Start(ctx))
	require.NotNil(t, rt)
	require.NotNil(t, logger)
	require.NotNil(t, reg)
	require.NotNil(t, gatherer)
	assert.Equal(t, observability.Ready{}, ready)

	mux := http.NewServeMux()
	rt.MountAdminRoutes(mux)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "obsfx_test_events_total 1")

	require.NoError(t, app.Stop(ctx))

	// 反事实校验：obsfx.Module 严禁向 Fx 容器注入 connect.Interceptor 或 connect.UnaryInterceptorFunc
	var interceptor connect.Interceptor
	badApp := fx.New(
		fx.Supply(fx.Annotate(cfg, fx.As(new(observability.Config)))),
		obsfx.Module,
		fx.Populate(&interceptor),
		fx.NopLogger,
	)
	require.Error(t, badApp.Err(), "obsfx.Module 严禁注入 connect.Interceptor 以根治双倍日志")

	var unaryInterceptor connect.UnaryInterceptorFunc
	badApp2 := fx.New(
		fx.Supply(fx.Annotate(cfg, fx.As(new(observability.Config)))),
		obsfx.Module,
		fx.Populate(&unaryInterceptor),
		fx.NopLogger,
	)
	require.Error(t, badApp2.Err(), "obsfx.Module 严禁注入 connect.UnaryInterceptorFunc")
}
