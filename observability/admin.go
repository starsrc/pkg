package observability

// 独立分段导入：确保 muxguard.init() 在 net/http/pprof.init() 之前保存原始 http.DefaultServeMux，
// 随后在本包 init() 中恢复，彻底杜绝 pprof 包副作用污染全局默认路由表。
import "github.com/starsrc/pkg/observability/internal/muxguard"

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/pprof"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func init() {
	muxguard.Restore()
}

// DefaultReadyTimeout 是 /readyz 执行单轮就绪探针的默认超时上限。
const DefaultReadyTimeout = 2 * time.Second

// ReadyCheck 定义依赖组件（如 PostgreSQL 连接池、本地引擎）的就绪探活闭包。
type ReadyCheck func(ctx context.Context) error

// AdminRouteConfig 定义管理面 HTTP 路由树暴露的子功能开关契约（Config 与 EnvConfig 均天然实现该接口）。
type AdminRouteConfig interface {
	// IsMetricsEnabled 返回是否暴露 /metrics 端点。
	IsMetricsEnabled() bool
	// IsPprofEnabled 返回是否暴露 /debug/pprof/* 性能剖析路由。
	IsPprofEnabled() bool
}

// AdminConfig 是 AdminRouteConfig 的轻量值结构体实现（适用于无需完整 EnvConfig 的局部组装与单测）。
type AdminConfig struct {
	MetricsEnabled bool
	PprofEnabled   bool
}

// IsMetricsEnabled 返回是否暴露 /metrics 端点。
func (c AdminConfig) IsMetricsEnabled() bool {
	return c.MetricsEnabled
}

// IsPprofEnabled 返回是否暴露 /debug/pprof/* 性能剖析路由。
func (c AdminConfig) IsPprofEnabled() bool {
	return c.PprofEnabled
}

type namedReadyCheck struct {
	name  string
	check ReadyCheck
}

type adminOptions struct {
	readyChecks  []namedReadyCheck
	readyTimeout time.Duration
}

// AdminOption 定义管理面路由挂载的配置选项接口。
type AdminOption interface {
	applyAdmin(*adminOptions)
}

// ReadyCheckOption 同时满足 Setup Option 与 AdminOption，允许在 Runtime 初始化或路由挂载阶段灵活注入探针。
type ReadyCheckOption interface {
	Option
	AdminOption
}

type readyCheckOpt struct {
	name  string
	check ReadyCheck
}

func (o readyCheckOpt) applySetup(s *setupOptions) {
	if o.check != nil && o.name != "" {
		s.readyChecks = append(s.readyChecks, namedReadyCheck(o))
	}
}

func (o readyCheckOpt) applyAdmin(a *adminOptions) {
	if o.check != nil && o.name != "" {
		a.readyChecks = append(a.readyChecks, namedReadyCheck(o))
	}
}

// WithReadyCheck 注册命名的外部组件就绪检查函数（由 /readyz 并发执行并受 2s 超时保护）。
func WithReadyCheck(name string, check ReadyCheck) ReadyCheckOption {
	return readyCheckOpt{name: name, check: check}
}

type adminOptFunc func(*adminOptions)

func (fn adminOptFunc) applyAdmin(a *adminOptions) {
	fn(a)
}

// WithReadyTimeout 覆盖 /readyz 的默认探活超时时长（默认 2s）。
func WithReadyTimeout(d time.Duration) AdminOption {
	return adminOptFunc(func(a *adminOptions) {
		if d > 0 {
			a.readyTimeout = d
		}
	})
}

type healthResponse struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks,omitempty"`
}

// RegisterAdminRoutes 在显式传入的 *http.ServeMux 上挂载管理面端点：
// - /metrics：当 cfg.MetricsEnabled 为 true 且 gatherer 非 nil 时暴露 Prometheus + OpenMetrics（支持 Trace Exemplar）；
// - /debug/pprof/*：当 cfg.PprofEnabled 为 true 时显式注册 Index/Cmdline/Profile/Symbol/Trace（绝不污染 http.DefaultServeMux）；
// - /healthz：存活探针（始终返回 200 JSON）；
// - /readyz：就绪探针（带 2s 超时并发执行所有 ReadyCheck，任一失败返回 503 JSON）。
func RegisterAdminRoutes(mux *http.ServeMux, gatherer prometheus.Gatherer, cfg AdminRouteConfig, opts ...AdminOption) {
	if mux == nil {
		return
	}

	options := adminOptions{
		readyTimeout: DefaultReadyTimeout,
	}
	for _, opt := range opts {
		if opt != nil {
			opt.applyAdmin(&options)
		}
	}

	metricsEnabled := true
	pprofEnabled := true
	if cfg != nil {
		metricsEnabled = cfg.IsMetricsEnabled()
		pprofEnabled = cfg.IsPprofEnabled()
	}

	if metricsEnabled && gatherer != nil {
		mux.Handle("/metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{
			EnableOpenMetrics: true,
		}))
	}

	if pprofEnabled {
		mux.HandleFunc("GET /debug/pprof/", pprof.Index)
		mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	}

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, healthResponse{Status: "ok"})
	})

	checks := append([]namedReadyCheck(nil), options.readyChecks...)
	timeout := options.readyTimeout

	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if len(checks) == 0 {
			writeJSON(w, http.StatusOK, healthResponse{Status: "ok"})
			return
		}

		checkCtx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()

		results := make(map[string]string, len(checks))
		var (
			mu        sync.Mutex
			wg        sync.WaitGroup
			anyFailed bool
		)

		wg.Add(len(checks))
		for _, item := range checks {
			go func(nc namedReadyCheck) {
				defer wg.Done()
				err := nc.check(checkCtx)
				mu.Lock()
				if err != nil {
					anyFailed = true
					results[nc.name] = err.Error()
				} else {
					results[nc.name] = "ok"
				}
				mu.Unlock()
			}(item)
		}
		wg.Wait()

		if anyFailed {
			writeJSON(w, http.StatusServiceUnavailable, healthResponse{
				Status: "unavailable",
				Checks: results,
			})
			return
		}
		writeJSON(w, http.StatusOK, healthResponse{
			Status: "ok",
			Checks: results,
		})
	})
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}
