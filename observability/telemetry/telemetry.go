package telemetry

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// NewResource 根据配置构建 OpenTelemetry Resource（服务名空值自动兜底为 "unknown-service"）。
func NewResource(cfg Config) (*resource.Resource, error) {
	serviceName := "unknown-service"
	var attrs []attribute.KeyValue
	if cfg != nil {
		if name := strings.TrimSpace(cfg.ServiceName()); name != "" {
			serviceName = name
		}
		if ver := strings.TrimSpace(cfg.ServiceVersion()); ver != "" {
			attrs = append(attrs, semconv.ServiceVersion(ver))
		}
		if env := strings.TrimSpace(cfg.DeploymentEnvironment()); env != "" {
			attrs = append(attrs, attribute.String("deployment.environment.name", env))
		}
	}
	attrs = append(attrs, semconv.ServiceName(serviceName))

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes("", attrs...),
	)
	if err != nil {
		return nil, fmt.Errorf("telemetry: failed to create otel resource: %w", err)
	}
	return res, nil
}

// NewTracerProvider 根据配置创建并返回 OpenTelemetry TracerProvider。
// - 若未开启（Enabled 为 false）或配置为空，则返回默认无 Exporter 的 TracerProvider；
// - 若 TracesExporter 为 "none"，则挂载 Resource 与 ParentBased(TraceIDRatioBased) 采样器，但不启动后台 OTLP Exporter 协程；
// - 若 TracesExporter 为 "otlp"（或空字符串），则挂载异步批量 OTLP gRPC Exporter。
func NewTracerProvider(ctx context.Context, cfg Config) (*sdktrace.TracerProvider, error) {
	if cfg == nil || !cfg.Enabled() {
		return sdktrace.NewTracerProvider(), nil
	}

	res, err := NewResource(cfg)
	if err != nil {
		return nil, err
	}

	sampler := sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio()))

	exporterType := strings.ToLower(strings.TrimSpace(cfg.TracesExporter()))
	if exporterType == "" {
		exporterType = ExporterOTLP
	}

	switch exporterType {
	case ExporterNone:
		return sdktrace.NewTracerProvider(
			sdktrace.WithResource(res),
			sdktrace.WithSampler(sampler),
		), nil
	case ExporterOTLP:
		endpoint := cfg.Endpoint()
		var opts []otlptracegrpc.Option
		if strings.Contains(endpoint, "://") {
			opts = append(opts, otlptracegrpc.WithEndpointURL(endpoint))
		} else {
			opts = append(opts, otlptracegrpc.WithEndpoint(endpoint))
			if cfg.Insecure() {
				opts = append(opts, otlptracegrpc.WithInsecure())
			}
		}

		exporter, err := otlptracegrpc.New(ctx, opts...)
		if err != nil {
			return nil, fmt.Errorf("telemetry: failed to create OTLP trace exporter: %w", err)
		}

		return sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exporter),
			sdktrace.WithResource(res),
			sdktrace.WithSampler(sampler),
		), nil
	default:
		return nil, fmt.Errorf("telemetry: unsupported traces exporter %q", exporterType)
	}
}

// InitTracer 根据配置初始化 OpenTelemetry TracerProvider，并注册全局 TracerProvider 与 W3C TraceContext/Baggage 传播器。
// 若未开启（Enabled 为 false）或配置为空，则保持当前全局状态不变并返回空操作 shutdown 函数。
func InitTracer(ctx context.Context, cfg Config) (func(context.Context) error, error) {
	if cfg == nil || !cfg.Enabled() {
		return func(context.Context) error { return nil }, nil
	}

	tp, err := NewTracerProvider(ctx, cfg)
	if err != nil {
		return nil, err
	}

	RegisterGlobalTracer(tp)
	return tp.Shutdown, nil
}

// RegisterGlobalTracer 将指定的 TracerProvider 与标准 W3C TraceContext + Baggage 传播器注册为全局默认实例。
func RegisterGlobalTracer(tp *sdktrace.TracerProvider) {
	if tp == nil {
		return
	}
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
}

// NewRegistry 创建实例级隔离的 *prometheus.Registry（严禁使用全局 DefaultRegisterer）。
func NewRegistry() *prometheus.Registry {
	return prometheus.NewRegistry()
}

// RegisterRuntimeCollectors 向给定的 Registerer 幂等注册 Go 运行时、进程与构建信息标准 Collector。
func RegisterRuntimeCollectors(reg prometheus.Registerer) error {
	if reg == nil {
		return errors.New("telemetry: nil prometheus registerer")
	}
	stdCollectors := []prometheus.Collector{
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collectors.NewBuildInfoCollector(),
	}
	for _, c := range stdCollectors {
		if err := reg.Register(c); err != nil {
			var alreadyReg prometheus.AlreadyRegisteredError
			if !errors.As(err, &alreadyReg) {
				return fmt.Errorf("telemetry: failed to register runtime collector: %w", err)
			}
		}
	}
	return nil
}

// NewMeterProvider 基于实例级隔离 *prometheus.Registry 构建 OTel MeterProvider，
// 预注册 Go/Process/BuildInfo Collector，并通过 OTel Prometheus Exporter（WithoutScopeInfo）桥接导出（含 Exemplar）。
// 若传入 reg 为 nil，则内部自动创建新的隔离 *prometheus.Registry。
func NewMeterProvider(cfg Config, reg *prometheus.Registry) (*sdkmetric.MeterProvider, error) {
	if reg == nil {
		reg = NewRegistry()
	}

	if err := RegisterRuntimeCollectors(reg); err != nil {
		return nil, err
	}

	res, err := NewResource(cfg)
	if err != nil {
		return nil, err
	}

	promExporter, err := otelprom.New(
		otelprom.WithRegisterer(reg),
		otelprom.WithoutScopeInfo(),
	)
	if err != nil {
		return nil, fmt.Errorf("telemetry: failed to create prometheus exporter: %w", err)
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(promExporter),
	)
	return mp, nil
}
