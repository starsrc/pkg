package serverx

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/starsrc/pkg/observability"
)

// AdminServerConfig 定义内网管理面 HTTP 服务的监听地址与超时契约（ServerConfig 及嵌入它的服务配置均天然满足）。
type AdminServerConfig interface {
	Config
	InternalServerAddress() string
}

// NewAdminMux 创建独立 *http.ServeMux 并显式挂载标准管理面端点（/metrics、/debug/pprof/*、/healthz、/readyz）。
func NewAdminMux(
	gatherer prometheus.Gatherer,
	cfg observability.AdminRouteConfig,
	opts ...observability.AdminOption,
) *http.ServeMux {
	mux := http.NewServeMux()
	observability.RegisterAdminRoutes(mux, gatherer, cfg, opts...)
	return mux
}

// NewAdminServer 基于 srvCfg.InternalServerAddress() 构建内网管理面 *http.Server（保持 WriteTimeout: 0 支撑长时 CPU Profile）。
func NewAdminServer(
	srvCfg AdminServerConfig,
	gatherer prometheus.Gatherer,
	cfg observability.AdminRouteConfig,
	opts ...observability.AdminOption,
) *http.Server {
	addr := "0.0.0.0:8090"
	if srvCfg != nil {
		if a := srvCfg.InternalServerAddress(); a != "" {
			addr = a
		}
	}
	mux := NewAdminMux(gatherer, cfg, opts...)
	return NewHTTPServer(addr, mux, srvCfg)
}
