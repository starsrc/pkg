package serverx_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/pkg/observability"
	"github.com/starsrc/pkg/serverx"
)

func TestNewAdminServer_And_NewAdminMux(t *testing.T) {
	reg := prometheus.NewRegistry()
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "serverx_admin_test_ready",
		Help: "test metric",
	})
	gauge.Set(1)
	require.NoError(t, reg.Register(gauge))

	srvCfg := serverx.ServerConfig{
		InternalServerHost:      "127.0.0.1",
		InternalServerPort:      9090,
		ServerReadHeaderTimeout: 5 * time.Second,
	}
	routeCfg := observability.AdminConfig{
		MetricsEnabled: true,
		PprofEnabled:   true,
	}

	adminSrv := serverx.NewAdminServer(
		srvCfg,
		reg,
		routeCfg,
		observability.WithReadyCheck("db", func(context.Context) error { return nil }),
	)
	require.NotNil(t, adminSrv)
	assert.Equal(t, "127.0.0.1:9090", adminSrv.Addr)
	assert.Equal(t, time.Duration(0), adminSrv.WriteTimeout, "管理面 HTTP 服务必须保持 WriteTimeout=0 以避免腰斩长时 CPU Profile")
	assert.Equal(t, 5*time.Second, adminSrv.ReadHeaderTimeout)

	for _, path := range []string{"/metrics", "/debug/pprof/", "/healthz", "/readyz"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		adminSrv.Handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code, "路径 %s 应返回 200 OK", path)
		if path == "/metrics" {
			assert.Contains(t, rec.Body.String(), "serverx_admin_test_ready 1")
		}
	}
}
