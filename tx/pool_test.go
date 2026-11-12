package tx_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/fx"

	"github.com/starsrc/pkg/tx"
)

type recordingLifecycle struct {
	hooks []fx.Hook
}

func (r *recordingLifecycle) Append(hook fx.Hook) {
	r.hooks = append(r.hooks, hook)
}

func TestPoolConfig_GettersAndValidation(t *testing.T) {
	t.Run("getters return configured values", func(t *testing.T) {
		cfg := tx.PoolConfig{
			DSN:               "postgres://user:pass@localhost:5432/testdb",
			MaxConns:          25,
			MinConns:          5,
			MaxConnLifetime:   2 * time.Hour,
			MaxConnIdleTime:   15 * time.Minute,
			HealthCheckPeriod: 30 * time.Second,
			TracingEnabled:    true,
		}

		assert.Equal(t, "postgres://user:pass@localhost:5432/testdb", cfg.GetDSN())
		assert.Equal(t, int32(25), cfg.DBMaxConns())
		assert.Equal(t, int32(5), cfg.DBMinConns())
		assert.Equal(t, 2*time.Hour, cfg.DBMaxConnLifetime())
		assert.Equal(t, 15*time.Minute, cfg.DBMaxConnIdleTime())
		assert.Equal(t, 30*time.Second, cfg.DBHealthCheckPeriod())
		assert.True(t, cfg.DBTracingEnabled())
		assert.False(t, cfg.DBMetricsEnabled())
		cfg.MetricsEnabled = true
		assert.True(t, cfg.DBMetricsEnabled())
	})

	t.Run("DBPoolConfig is a valid type alias for PoolConfig", func(t *testing.T) {
		var dbCfg tx.DBPoolConfig = tx.PoolConfig{
			DSN: "postgres://user:pass@localhost:5432/testdb",
		}
		assert.Equal(t, "postgres://user:pass@localhost:5432/testdb", dbCfg.GetDSN())
	})

	t.Run("validate rejects empty DSN", func(t *testing.T) {
		cfg := tx.PoolConfig{
			DSN: "",
		}
		err := cfg.Validate()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "DSN")
	})

	t.Run("validate rejects negative MaxConns or MinConns", func(t *testing.T) {
		cfg1 := tx.PoolConfig{
			DSN:      "postgres://localhost:5432/db",
			MaxConns: -1,
		}
		err1 := cfg1.Validate()
		require.Error(t, err1)
		assert.Contains(t, err1.Error(), "max conns")

		cfg2 := tx.PoolConfig{
			DSN:      "postgres://localhost:5432/db",
			MinConns: -1,
		}
		err2 := cfg2.Validate()
		require.Error(t, err2)
		assert.Contains(t, err2.Error(), "min conns")
	})

	t.Run("validate passes on valid config", func(t *testing.T) {
		cfg := tx.PoolConfig{
			DSN:      "postgres://localhost:5432/db",
			MaxConns: 10,
			MinConns: 2,
		}
		require.NoError(t, cfg.Validate())
	})
}

func TestOpenPool(t *testing.T) {
	t.Run("invalid DSN returns error", func(t *testing.T) {
		cfg := tx.PoolConfig{
			DSN: "postgres://%invalid-url",
		}
		pool, err := tx.OpenPool(cfg)
		require.Error(t, err)
		assert.Nil(t, pool)
		assert.Contains(t, err.Error(), "parse pool config")
	})

	t.Run("applies custom pool options and clamping", func(t *testing.T) {
		cfg := tx.PoolConfig{
			DSN:               "postgres://user:pass@127.0.0.1:5432/testdb",
			MaxConns:          10,
			MinConns:          25, // clamped to MaxConns (10)
			MaxConnLifetime:   45 * time.Minute,
			MaxConnIdleTime:   15 * time.Minute,
			HealthCheckPeriod: 30 * time.Second,
			TracingEnabled:    true,
		}

		pool, err := tx.OpenPool(cfg)
		require.NoError(t, err)
		require.NotNil(t, pool)
		defer pool.Close()

		poolCfg := pool.Config()
		assert.Equal(t, int32(10), poolCfg.MaxConns)
		assert.Equal(t, int32(10), poolCfg.MinConns, "MinConns should be clamped to MaxConns")
		assert.Equal(t, 45*time.Minute, poolCfg.MaxConnLifetime)
		assert.Equal(t, 15*time.Minute, poolCfg.MaxConnIdleTime)
		assert.Equal(t, 30*time.Second, poolCfg.HealthCheckPeriod)
		assert.NotNil(t, poolCfg.ConnConfig.Tracer, "Tracer should be attached when TracingEnabled is true")
	})

	t.Run("tracing disabled leaves tracer nil", func(t *testing.T) {
		cfg := tx.PoolConfig{
			DSN:            "postgres://user:pass@127.0.0.1:5432/testdb",
			MaxConns:       20,
			MinConns:       5,
			TracingEnabled: false,
		}

		pool, err := tx.OpenPool(cfg)
		require.NoError(t, err)
		require.NotNil(t, pool)
		defer pool.Close()

		poolCfg := pool.Config()
		assert.Equal(t, int32(20), poolCfg.MaxConns)
		assert.Equal(t, int32(5), poolCfg.MinConns)
		assert.Nil(t, poolCfg.ConnConfig.Tracer, "Tracer should be nil when TracingEnabled is false")
	})

	t.Run("MetricsEnabled registers pgxpool stats onto global MeterProvider", func(t *testing.T) {
		origMP := otel.GetMeterProvider()
		reader := sdkmetric.NewManualReader()
		mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
		otel.SetMeterProvider(mp)
		t.Cleanup(func() {
			otel.SetMeterProvider(origMP)
			_ = mp.Shutdown(context.Background())
		})

		cfg := tx.PoolConfig{
			DSN:            "postgres://user:pass@127.0.0.1:5432/metricsdb",
			MaxConns:       12,
			MetricsEnabled: true,
		}
		pool, err := tx.OpenPool(cfg)
		require.NoError(t, err)
		require.NotNil(t, pool)
		defer pool.Close()

		var rm metricdata.ResourceMetrics
		require.NoError(t, reader.Collect(context.Background(), &rm))

		var metricNames []string
		for _, sm := range rm.ScopeMetrics {
			for _, m := range sm.Metrics {
				metricNames = append(metricNames, m.Name)
			}
		}
		assert.Contains(t, metricNames, "pgxpool.max_connections", "开启 MetricsEnabled 时应导出 otelpgx 连接池状态指标")
	})
}

func TestRegisterPoolLifecycle(t *testing.T) {
	t.Run("nil arguments do not panic", func(t *testing.T) {
		assert.NotPanics(t, func() {
			tx.RegisterPoolLifecycle(nil, nil)
		})
	})

	t.Run("OnStart ping failure closes pool and returns error", func(t *testing.T) {
		// Use an unreachable port to guarantee ping failure
		cfg := tx.PoolConfig{
			DSN: "postgres://user:pass@127.0.0.1:59999/testdb?connect_timeout=1",
		}
		pool, err := tx.OpenPool(cfg)
		require.NoError(t, err)
		require.NotNil(t, pool)

		lc := &recordingLifecycle{}
		tx.RegisterPoolLifecycle(lc, pool)
		require.Len(t, lc.hooks, 1)

		hook := lc.hooks[0]
		require.NotNil(t, hook.OnStart)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		startErr := hook.OnStart(ctx)
		require.Error(t, startErr)
		assert.Contains(t, startErr.Error(), "ping database on start")

		// Verify pool was closed on ping error
		_, acquireErr := pool.Acquire(context.Background())
		require.Error(t, acquireErr)
		assert.Contains(t, acquireErr.Error(), "closed pool")
	})

	t.Run("OnStop closes pool", func(t *testing.T) {
		cfg := tx.PoolConfig{
			DSN: "postgres://user:pass@127.0.0.1:5432/testdb",
		}
		pool, err := tx.OpenPool(cfg)
		require.NoError(t, err)
		require.NotNil(t, pool)

		lc := &recordingLifecycle{}
		tx.RegisterPoolLifecycle(lc, pool)
		require.Len(t, lc.hooks, 1)

		hook := lc.hooks[0]
		require.NotNil(t, hook.OnStop)

		stopErr := hook.OnStop(context.Background())
		assert.NoError(t, stopErr)

		// Verify pool is closed
		_, acquireErr := pool.Acquire(context.Background())
		require.Error(t, acquireErr)
		assert.Contains(t, acquireErr.Error(), "closed pool")
	})
}

func TestProvidePool(t *testing.T) {
	t.Run("returns error on invalid DSN", func(t *testing.T) {
		cfg := tx.PoolConfig{
			DSN: "postgres://%invalid-url",
		}
		lc := &recordingLifecycle{}
		pool, err := tx.ProvidePool(lc, cfg)
		require.Error(t, err)
		assert.Nil(t, pool)
		assert.Empty(t, lc.hooks)
	})

	t.Run("creates pool and registers lifecycle", func(t *testing.T) {
		cfg := tx.PoolConfig{
			DSN: "postgres://user:pass@127.0.0.1:5432/testdb",
		}
		lc := &recordingLifecycle{}
		pool, err := tx.ProvidePool(lc, cfg)
		require.NoError(t, err)
		require.NotNil(t, pool)

		require.Len(t, lc.hooks, 1)
		require.NotNil(t, lc.hooks[0].OnStart)
		require.NotNil(t, lc.hooks[0].OnStop)

		err = lc.hooks[0].OnStop(context.Background())
		require.NoError(t, err)

		_, acquireErr := pool.Acquire(context.Background())
		require.Error(t, acquireErr)
		assert.Contains(t, acquireErr.Error(), "closed pool")
	})
}
