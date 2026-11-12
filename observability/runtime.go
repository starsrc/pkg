package observability

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/starsrc/pkg/observability/slogx"
	"github.com/starsrc/pkg/observability/telemetry"
)

type setupOptions struct {
	logWriter        io.Writer
	setDefaultLogger bool
	setGlobalOTel    bool
	collectors       []prometheus.Collector
	readyChecks      []namedReadyCheck
}

// Option 定义初始化 Runtime 的配置选项接口。
type Option interface {
	applySetup(*setupOptions)
}

type setupOptFunc func(*setupOptions)

func (fn setupOptFunc) applySetup(s *setupOptions) {
	fn(s)
}

// WithLogWriter 指定结构化日志的输出目标（如 CLI 使用 os.Stderr 或守护进程日志文件）。
func WithLogWriter(w io.Writer) Option {
	return setupOptFunc(func(s *setupOptions) {
		if w != nil {
			s.logWriter = w
		}
	})
}

// WithSetDefaultLogger 控制是否将初始化的 *slog.Logger 设为全局 slog.Default()（默认 true）。
func WithSetDefaultLogger(set bool) Option {
	return setupOptFunc(func(s *setupOptions) {
		s.setDefaultLogger = set
	})
}

// WithSetGlobalOTel 控制是否将初始化的 MeterProvider / TracerProvider / Propagator 注册为全局默认实例（默认 true）。
func WithSetGlobalOTel(set bool) Option {
	return setupOptFunc(func(s *setupOptions) {
		s.setGlobalOTel = set
	})
}

// WithCollectors 在 Runtime 初始化阶段向实例级隔离 Registry 预注册自定义 Prometheus Collector。
func WithCollectors(cs ...prometheus.Collector) Option {
	return setupOptFunc(func(s *setupOptions) {
		for _, c := range cs {
			if c != nil {
				s.collectors = append(s.collectors, c)
			}
		}
	})
}

// Runtime 承载纯 Go（无 Fx 依赖）的可观测性运行时组件及其完整生命周期。
type Runtime struct {
	logger         *slog.Logger
	registry       *prometheus.Registry
	meterProvider  *sdkmetric.MeterProvider
	tracerProvider *sdktrace.TracerProvider
	routeCfg       AdminRouteConfig
	readyChecks    []namedReadyCheck
	adminLn        net.Listener
	adminSrv       *http.Server
	adminAddr      string
	shutdownOnce   sync.Once
	shutdownErr    error
}

// Setup 根据统一配置初始化日志、隔离 Prometheus Registry、OTel MeterProvider、TracerProvider 及可选独立管理 HTTP 服务。
func Setup(ctx context.Context, cfg Config, opts ...Option) (*Runtime, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if cfg == nil {
		def := DefaultConfig()
		cfg = def
	}

	if validator, ok := cfg.(interface{ Validate() error }); ok {
		if err := validator.Validate(); err != nil {
			return nil, err
		}
	} else if r := cfg.SampleRatio(); math.IsNaN(r) || math.IsInf(r, 0) || r < 0 || r > 1 {
		return nil, fmt.Errorf("invalid sample ratio %v: must be between 0.0 and 1.0", r)
	}

	options := setupOptions{
		setDefaultLogger: true,
		setGlobalOTel:    true,
	}
	for _, opt := range opts {
		if opt != nil {
			opt.applySetup(&options)
		}
	}

	// 1. 初始化结构化日志
	var slogOpts []slogx.Option
	if options.logWriter != nil {
		slogOpts = append(slogOpts, slogx.WithWriter(options.logWriter))
	}
	slogOpts = append(slogOpts, slogx.WithSetDefault(options.setDefaultLogger))
	logger := slogx.New(cfg, slogOpts...)

	// 2. 初始化实例级隔离 Prometheus Registry 与 OTel MeterProvider
	reg := telemetry.NewRegistry()
	var mp *sdkmetric.MeterProvider
	if cfg.IsMetricsEnabled() {
		var err error
		mp, err = telemetry.NewMeterProvider(cfg, reg)
		if err != nil {
			return nil, err
		}
		if options.setGlobalOTel {
			otel.SetMeterProvider(mp)
		}
	}

	for _, c := range options.collectors {
		if err := reg.Register(c); err != nil {
			if mp != nil {
				_ = mp.Shutdown(ctx)
			}
			return nil, fmt.Errorf("observability: register collector: %w", err)
		}
	}

	// 3. 初始化 OTel TracerProvider 与全局 W3C Propagator
	var tp *sdktrace.TracerProvider
	if cfg.Enabled() {
		var err error
		tp, err = telemetry.NewTracerProvider(ctx, cfg)
		if err != nil {
			if mp != nil {
				_ = mp.Shutdown(ctx)
			}
			return nil, err
		}
		if options.setGlobalOTel {
			telemetry.RegisterGlobalTracer(tp)
		}
	}

	rt := &Runtime{
		logger:         logger,
		registry:       reg,
		meterProvider:  mp,
		tracerProvider: tp,
		routeCfg: AdminConfig{
			MetricsEnabled: cfg.IsMetricsEnabled(),
			PprofEnabled:   cfg.IsPprofEnabled(),
		},
		readyChecks: append([]namedReadyCheck(nil), options.readyChecks...),
	}

	// 4. 按需启动独立回环/后台管理 HTTP 服务（WriteTimeout: 0 支持长时 CPU Profile）
	adminAddr := strings.TrimSpace(cfg.GetAdminAddr())
	if adminAddr != "" && !strings.EqualFold(adminAddr, "off") {
		ln, err := net.Listen("tcp", adminAddr)
		if err != nil {
			_ = rt.shutdownProviders(ctx)
			return nil, fmt.Errorf("observability: listen admin addr %q: %w", adminAddr, err)
		}

		mux := http.NewServeMux()
		rt.MountAdminRoutes(mux)

		srv := &http.Server{
			Addr:              ln.Addr().String(),
			Handler:           mux,
			ReadHeaderTimeout: 10 * time.Second,
			WriteTimeout:      0,
		}
		rt.adminLn = ln
		rt.adminSrv = srv
		rt.adminAddr = ln.Addr().String()

		go func() {
			_ = srv.Serve(ln)
		}()
	}

	return rt, nil
}

// Logger 返回已挂载 OtelHandler 的结构化 *slog.Logger。
func (r *Runtime) Logger() *slog.Logger {
	if r == nil {
		return slog.Default()
	}
	return r.logger
}

// Registry 返回实例级隔离的 *prometheus.Registry（始终非 nil，即使 MetricsDisabled 亦可安全调用）。
func (r *Runtime) Registry() *prometheus.Registry {
	if r == nil || r.registry == nil {
		return telemetry.NewRegistry()
	}
	return r.registry
}

// Gatherer 返回关联的 prometheus.Gatherer。
func (r *Runtime) Gatherer() prometheus.Gatherer {
	return r.Registry()
}

// MeterProvider 返回底层 *sdkmetric.MeterProvider（若未开启 Metrics 则为 nil）。
func (r *Runtime) MeterProvider() *sdkmetric.MeterProvider {
	if r == nil {
		return nil
	}
	return r.meterProvider
}

// TracerProvider 返回底层 *sdktrace.TracerProvider（若未开启 Tracing 则为 nil）。
func (r *Runtime) TracerProvider() *sdktrace.TracerProvider {
	if r == nil {
		return nil
	}
	return r.tracerProvider
}

// RegisterCollector 向当前 Runtime 的隔离 Registry 注册一个或多个自定义 Collector。
func (r *Runtime) RegisterCollector(cs ...prometheus.Collector) error {
	if r == nil || r.registry == nil {
		return errors.New("observability: nil runtime")
	}
	for _, c := range cs {
		if c == nil {
			continue
		}
		if err := r.registry.Register(c); err != nil {
			return fmt.Errorf("observability: register collector: %w", err)
		}
	}
	return nil
}

// AdminAddr 返回独立管理 HTTP 服务的实际监听地址（当配置为 "127.0.0.1:0" 时返回内核分配的真实端口地址；未开启时返回 ""）。
func (r *Runtime) AdminAddr() string {
	if r == nil {
		return ""
	}
	return r.adminAddr
}

// MountAdminRoutes 将当前 Runtime 的 /metrics、/debug/pprof/*、/healthz 与 /readyz 挂载至外部 *http.ServeMux。
func (r *Runtime) MountAdminRoutes(mux *http.ServeMux, opts ...AdminOption) {
	if r == nil || mux == nil {
		return
	}
	mergedOpts := make([]AdminOption, 0, len(r.readyChecks)+len(opts))
	for _, nc := range r.readyChecks {
		mergedOpts = append(mergedOpts, WithReadyCheck(nc.name, nc.check))
	}
	mergedOpts = append(mergedOpts, opts...)
	RegisterAdminRoutes(mux, r.Gatherer(), r.routeCfg, mergedOpts...)
}

// Shutdown 逆序优雅关闭独立管理 HTTP 服务、MeterProvider 与 TracerProvider（幂等安全）。
func (r *Runtime) Shutdown(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.shutdownOnce.Do(func() {
		var errs error
		if r.adminSrv != nil {
			if err := r.adminSrv.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errs = errors.Join(errs, fmt.Errorf("admin server shutdown: %w", err))
				if closeErr := r.adminSrv.Close(); closeErr != nil && !errors.Is(closeErr, http.ErrServerClosed) {
					errs = errors.Join(errs, fmt.Errorf("admin server close: %w", closeErr))
				}
			}
		}
		if err := r.shutdownProviders(ctx); err != nil {
			errs = errors.Join(errs, err)
		}
		r.shutdownErr = errs
	})
	return r.shutdownErr
}

func (r *Runtime) shutdownProviders(ctx context.Context) error {
	var errs error
	if r.meterProvider != nil {
		if err := r.meterProvider.Shutdown(ctx); err != nil {
			errs = errors.Join(errs, fmt.Errorf("meter provider shutdown: %w", err))
		}
	}
	if r.tracerProvider != nil {
		if err := r.tracerProvider.Shutdown(ctx); err != nil {
			errs = errors.Join(errs, fmt.Errorf("tracer provider shutdown: %w", err))
		}
	}
	return errs
}
