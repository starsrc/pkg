package tx

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
)

// PoolConfig 承载数据库连接池基础配置。
type PoolConfig struct {
	DSN               string        `env:"DB_DSN,required,notEmpty"`
	MaxConns          int32         `env:"DB_MAX_CONNS"`
	MinConns          int32         `env:"DB_MIN_CONNS"`
	MaxConnLifetime   time.Duration `env:"DB_MAX_CONN_LIFETIME" envDefault:"1h"`
	MaxConnIdleTime   time.Duration `env:"DB_MAX_CONN_IDLE_TIME" envDefault:"30m"`
	HealthCheckPeriod time.Duration `env:"DB_HEALTH_CHECK_PERIOD" envDefault:"1m"`
	TracingEnabled    bool          `env:"DB_TRACING_ENABLED" envDefault:"true"`
	MetricsEnabled    bool          `env:"DB_METRICS_ENABLED" envDefault:"true"`
}

// DBPoolConfig 为 PoolConfig 的类型别名，用于兼容各服务既有命名。
type DBPoolConfig = PoolConfig

// GetDSN 返回数据库连接串。
func (c PoolConfig) GetDSN() string {
	return c.DSN
}

// DBMaxConns 返回最大连接数。
func (c PoolConfig) DBMaxConns() int32 {
	return c.MaxConns
}

// DBMinConns 返回最小连接数。
func (c PoolConfig) DBMinConns() int32 {
	return c.MinConns
}

// DBMaxConnLifetime 返回连接最大存活时长。
func (c PoolConfig) DBMaxConnLifetime() time.Duration {
	return c.MaxConnLifetime
}

// DBMaxConnIdleTime 返回空闲连接最大保持时长。
func (c PoolConfig) DBMaxConnIdleTime() time.Duration {
	return c.MaxConnIdleTime
}

// DBHealthCheckPeriod 返回连接池健康探活周期。
func (c PoolConfig) DBHealthCheckPeriod() time.Duration {
	return c.HealthCheckPeriod
}

// DBTracingEnabled 返回是否开启 OpenTelemetry 链路追踪。
func (c PoolConfig) DBTracingEnabled() bool {
	return c.TracingEnabled
}

// DBMetricsEnabled 返回是否开启连接池状态指标采集。
func (c PoolConfig) DBMetricsEnabled() bool {
	return c.MetricsEnabled
}

// Validate 校验连接池关键参数的合法性。
func (c PoolConfig) Validate() error {
	if c.DSN == "" {
		return errors.New("database DSN is required")
	}
	if c.MaxConns < 0 {
		return errors.New("database max conns must be non-negative")
	}
	if c.MinConns < 0 {
		return errors.New("database min conns must be non-negative")
	}
	return nil
}

// OpenPool 根据 PoolConfig 配置初始化底层 *pgxpool.Pool。
func OpenPool(cfg PoolConfig) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse pool config: %w", err)
	}

	if cfg.TracingEnabled {
		poolCfg.ConnConfig.Tracer = otelpgx.NewTracer()
	}

	if cfg.MaxConns > 0 {
		poolCfg.MaxConns = cfg.MaxConns
	}

	minConns := cfg.MinConns
	if cfg.MaxConns > 0 && minConns > cfg.MaxConns {
		minConns = cfg.MaxConns
	}
	if minConns > 0 {
		poolCfg.MinConns = minConns
	}

	if poolCfg.MaxConns > 0 && poolCfg.MinConns > poolCfg.MaxConns {
		poolCfg.MinConns = poolCfg.MaxConns
	}

	if cfg.MaxConnLifetime > 0 {
		poolCfg.MaxConnLifetime = cfg.MaxConnLifetime
	}
	if cfg.MaxConnIdleTime > 0 {
		poolCfg.MaxConnIdleTime = cfg.MaxConnIdleTime
	}
	if cfg.HealthCheckPeriod > 0 {
		poolCfg.HealthCheckPeriod = cfg.HealthCheckPeriod
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	if cfg.MetricsEnabled {
		if err := otelpgx.RecordStats(pool); err != nil {
			pool.Close()
			return nil, fmt.Errorf("record pool stats: %w", err)
		}
	}

	return pool, nil
}

// RegisterPoolLifecycle 将连接池生命周期注册至 Fx：
// OnStart 执行 pool.Ping(ctx)，若失败则立即调用 pool.Close() 并返回错误；
// OnStop 调用 pool.Close()。
func RegisterPoolLifecycle(lc fx.Lifecycle, pool *pgxpool.Pool) {
	if lc == nil || pool == nil {
		return
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			if err := pool.Ping(ctx); err != nil {
				pool.Close()
				return fmt.Errorf("failed to ping database on start: %w", err)
			}
			return nil
		},
		OnStop: func(context.Context) error {
			pool.Close()
			return nil
		},
	})
}

// ProvidePool 作为 Fx provider 函数，初始化连接池并注册生命周期钩子。
func ProvidePool(lc fx.Lifecycle, cfg PoolConfig) (*pgxpool.Pool, error) {
	pool, err := OpenPool(cfg)
	if err != nil {
		return nil, err
	}
	RegisterPoolLifecycle(lc, pool)
	return pool, nil
}
