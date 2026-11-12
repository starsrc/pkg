package telemetry_test

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace/noop"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"

	"github.com/starsrc/pkg/observability/telemetry"
)

type mockTraceServer struct {
	coltracepb.UnimplementedTraceServiceServer
	mu       sync.Mutex
	requests []*coltracepb.ExportTraceServiceRequest
}

func (s *mockTraceServer) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	s.mu.Lock()
	s.requests = append(s.requests, req)
	s.mu.Unlock()
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

func (s *mockTraceServer) spanCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, req := range s.requests {
		for _, rs := range req.ResourceSpans {
			for _, ss := range rs.ScopeSpans {
				count += len(ss.Spans)
			}
		}
	}
	return count
}

func (s *mockTraceServer) serviceNames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var names []string
	for _, req := range s.requests {
		for _, rs := range req.ResourceSpans {
			if rs.Resource != nil {
				for _, attr := range rs.Resource.Attributes {
					if attr.Key == "service.name" {
						names = append(names, attr.Value.GetStringValue())
					}
				}
			}
		}
	}
	return names
}

func startMockOTLPServer(t *testing.T) (*mockTraceServer, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	mockSrv := &mockTraceServer{}
	coltracepb.RegisterTraceServiceServer(srv, mockSrv)

	go func() {
		_ = srv.Serve(ln)
	}()

	t.Cleanup(func() {
		srv.GracefulStop()
		_ = ln.Close()
	})

	return mockSrv, ln.Addr().String()
}

type testConfig struct {
	enabled        bool
	serviceName    string
	serviceVersion string
	deployEnv      string
	tracesExporter string
	endpoint       string
	insecure       bool
	sampleRatio    float64
	trustRemote    bool
}

func (c *testConfig) Enabled() bool                 { return c.enabled }
func (c *testConfig) ServiceName() string           { return c.serviceName }
func (c *testConfig) ServiceVersion() string        { return c.serviceVersion }
func (c *testConfig) DeploymentEnvironment() string { return c.deployEnv }
func (c *testConfig) TracesExporter() string        { return c.tracesExporter }
func (c *testConfig) Endpoint() string              { return c.endpoint }
func (c *testConfig) Insecure() bool                { return c.insecure }
func (c *testConfig) SampleRatio() float64          { return c.sampleRatio }
func (c *testConfig) TrustRemote() bool             { return c.trustRemote }

var _ telemetry.Config = (*testConfig)(nil)

func TestInitTracer_DisabledOrNil(t *testing.T) {
	ctx := context.Background()

	origTP := otel.GetTracerProvider()
	origProp := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(origTP)
		otel.SetTextMapPropagator(origProp)
	})

	t.Run("nil config returns no-op shutdown and preserves global state", func(t *testing.T) {
		customTP := sdktrace.NewTracerProvider()
		defer func() { _ = customTP.Shutdown(ctx) }()
		otel.SetTracerProvider(customTP)

		shutdown, err := telemetry.InitTracer(ctx, nil)
		require.NoError(t, err)
		require.NotNil(t, shutdown)
		assert.Same(t, customTP, otel.GetTracerProvider())
		assert.NoError(t, shutdown(ctx))
	})

	t.Run("disabled config returns no-op shutdown and preserves global state", func(t *testing.T) {
		customTP := sdktrace.NewTracerProvider()
		defer func() { _ = customTP.Shutdown(ctx) }()
		otel.SetTracerProvider(customTP)

		cfg := &testConfig{enabled: false}
		shutdown, err := telemetry.InitTracer(ctx, cfg)
		require.NoError(t, err)
		require.NotNil(t, shutdown)
		assert.Same(t, customTP, otel.GetTracerProvider())
		assert.NoError(t, shutdown(ctx))
	})
}

func TestNewTracerProvider_ExporterNone_TraceparentPropagation(t *testing.T) {
	ctx := context.Background()
	origTP := otel.GetTracerProvider()
	origProp := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(origTP)
		otel.SetTextMapPropagator(origProp)
	})

	otel.SetTracerProvider(noop.NewTracerProvider())
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())

	// 指向不存在端口证明 ExporterNone 完全不发起任何网络连接，且 sampleRatio=0.0 时依然产生有效 TraceID/SpanID 注入 traceparent
	cfg := &testConfig{
		enabled:        true,
		serviceName:    "bowl-client",
		tracesExporter: telemetry.ExporterNone,
		endpoint:       "127.0.0.1:1",
		sampleRatio:    0.0,
	}

	shutdown, err := telemetry.InitTracer(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = shutdown(ctx) })

	spanCtx, span := otel.GetTracerProvider().Tracer("bowl").Start(ctx, "rpc.lookup")
	defer span.End()

	sc := span.SpanContext()
	assert.True(t, sc.IsValid(), "OTEL_TRACES_EXPORTER=none 下即使未命中采样也必须生成合法 128-bit TraceID 与 64-bit SpanID")
	assert.False(t, sc.IsSampled(), "sampleRatio=0.0 时采样标志位应为 00")

	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(spanCtx, carrier)
	tpHeader := carrier.Get("traceparent")
	require.NotEmpty(t, tpHeader)
	assert.True(t, strings.HasPrefix(tpHeader, "00-"+sc.TraceID().String()+"-"+sc.SpanID().String()+"-00"), "实际 traceparent: %s", tpHeader)
	assert.Contains(t, otel.GetTextMapPropagator().Fields(), "baggage")
}

func TestNewTracerProvider_Enabled_OTLP(t *testing.T) {
	ctx := context.Background()

	mockSrv, mockAddr := startMockOTLPServer(t)
	cfg := &testConfig{
		enabled:        true,
		serviceName:    "auth-test-service",
		tracesExporter: telemetry.ExporterOTLP,
		endpoint:       mockAddr,
		insecure:       true,
		sampleRatio:    1.0,
		trustRemote:    true,
	}

	tp, err := telemetry.NewTracerProvider(ctx, cfg)
	require.NoError(t, err)
	require.NotNil(t, tp)

	_, span := tp.Tracer("test").Start(ctx, "test-span-sampled")
	assert.True(t, span.IsRecording())
	span.End()

	shutdownCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	require.NoError(t, tp.Shutdown(shutdownCtx))

	assert.Equal(t, 1, mockSrv.spanCount(), "sampleRatio=1.0 时 Span 应被正常导出")
	assert.Contains(t, mockSrv.serviceNames(), "auth-test-service", "导出的 Resource 中应包含配置的 service.name")
}

func TestNewMeterProvider_IsolatedRegistryAndWithoutScopeInfo(t *testing.T) {
	ctx := context.Background()

	// 并发创建多个独立 Registry + MeterProvider 严禁冲突 panic
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reg := telemetry.NewRegistry()
			mp, err := telemetry.NewMeterProvider(&testConfig{
				serviceName:    "test-metrics-svc",
				serviceVersion: "1.0.0",
			}, reg)
			require.NoError(t, err)
			t.Cleanup(func() { _ = mp.Shutdown(ctx) })

			counter, err := mp.Meter("test.scope").Int64Counter("demo.requests")
			require.NoError(t, err)
			counter.Add(ctx, 3, metric.WithAttributes(attribute.String("op", "lookup")))

			mfs, err := reg.Gather()
			require.NoError(t, err)

			names := make(map[string]bool, len(mfs))
			for _, mf := range mfs {
				names[mf.GetName()] = true
				if mf.GetName() == "demo_requests_total" {
					for _, m := range mf.GetMetric() {
						for _, lp := range m.GetLabel() {
							assert.NotEqual(t, "otel_scope_name", lp.GetName(), "WithoutScopeInfo 开启时不得注入 otel_scope_name 标签")
						}
					}
				}
			}

			assert.True(t, names["go_goroutines"], "应预注册 GoCollector")
			assert.True(t, names["go_info"], "应预注册 BuildInfoCollector")
			assert.True(t, names["demo_requests_total"], "OTel Counter 应桥接导出至隔离 Prometheus Registry")
		}()
	}
	wg.Wait()

	// 验证全局 DefaultGatherer 未被污染（我们的 demo_requests_total 不在全局 Registry 中）
	defaultMFs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, mf := range defaultMFs {
		assert.NotEqual(t, "demo_requests_total", mf.GetName())
	}
}
