package tx

import (
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	// DefaultMaxAttempts 默认最大事务尝试次数（初始 1 次 + 重试 15 次）。
	DefaultMaxAttempts = 16
	// DefaultInitialBackoff 默认初始重试回退延迟。
	DefaultInitialBackoff = 5 * time.Millisecond
	// DefaultMaxBackoff 默认最大重试回退延迟。
	DefaultMaxBackoff = 50 * time.Millisecond
)

const (
	// SQLStateSerializationFailure PostgreSQL 事务串行化失败错误码 (40001)。
	SQLStateSerializationFailure = "40001"
	// SQLStateDeadlockDetected PostgreSQL 死锁检测错误码 (40P01)。
	SQLStateDeadlockDetected = "40P01"
)

// IsoLevel 定义事务隔离级别枚举。
type IsoLevel string

const (
	// ReadUncommitted 读未提交隔离级别。
	ReadUncommitted IsoLevel = "ReadUncommitted"
	// ReadCommitted 读已提交隔离级别。
	ReadCommitted IsoLevel = "ReadCommitted"
	// RepeatableRead 可重复读隔离级别。
	RepeatableRead IsoLevel = "RepeatableRead"
	// Serializable 可串行化隔离级别。
	Serializable IsoLevel = "Serializable"
)

// AccessMode 定义事务访问模式枚举。
type AccessMode string

const (
	// ReadWrite 读写模式。
	ReadWrite AccessMode = "ReadWrite"
	// ReadOnly 只读模式。
	ReadOnly AccessMode = "ReadOnly"
)

// Options 封装事务运行配置与重试策略。
type Options struct {
	IsoLevel       IsoLevel
	AccessMode     AccessMode
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	ExtraRetryable []error
}

// Option 定义修改事务选项的函数式配置项。
type Option func(*Options)

// WithIsoLevel 设置事务隔离级别。
func WithIsoLevel(level IsoLevel) Option {
	return func(o *Options) {
		o.IsoLevel = level
	}
}

// WithReadOnly 设置事务访问模式为只读。
func WithReadOnly() Option {
	return func(o *Options) {
		o.AccessMode = ReadOnly
	}
}

// WithReadWrite 设置事务访问模式为读写。
func WithReadWrite() Option {
	return func(o *Options) {
		o.AccessMode = ReadWrite
	}
}

// WithRetry 配置事务指数回退重试策略。
// 当 maxAttempts > 1 时启用重试；当 maxBackoff 小于 initialBackoff 且大于 0 时，自动修正 maxBackoff 为 initialBackoff。
func WithRetry(maxAttempts int, initialBackoff, maxBackoff time.Duration, extraRetryable ...error) Option {
	return func(o *Options) {
		o.MaxAttempts = maxAttempts
		o.InitialBackoff = initialBackoff
		o.MaxBackoff = maxBackoff
		if o.MaxBackoff > 0 && o.InitialBackoff > o.MaxBackoff {
			o.MaxBackoff = o.InitialBackoff
		}
		o.ExtraRetryable = extraRetryable
	}
}

// WithDefaultRetry 使用默认配置（16 次最大尝试，5ms 初始回退翻倍至 50ms 上限）配置重试策略。
func WithDefaultRetry(extraRetryable ...error) Option {
	return WithRetry(DefaultMaxAttempts, DefaultInitialBackoff, DefaultMaxBackoff, extraRetryable...)
}

// IsRetryable 判断错误是否为可重试的事务暂时性错误。
// 支持识别 PostgreSQL SQLState 40001 (serialization_failure)、40P01 (deadlock_detected)，
// 以及调用方传入的 extraRetryable 哨兵错误列表。
func IsRetryable(err error, extraRetryable ...error) bool {
	if err == nil {
		return false
	}
	for _, target := range extraRetryable {
		if target != nil && errors.Is(err, target) {
			return true
		}
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == SQLStateSerializationFailure || pgErr.Code == SQLStateDeadlockDetected
	}
	return false
}

// ApplyOptions 将选项列表聚合为 Options 配置结构。
func ApplyOptions(opts ...Option) Options {
	var options Options
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}
	return options
}

// ToPGXTxOptions 将驱动无关的 Options 转换为 pgx.TxOptions。
func ToPGXTxOptions(opts Options) pgx.TxOptions {
	var pgxOpts pgx.TxOptions

	switch opts.IsoLevel {
	case ReadUncommitted:
		pgxOpts.IsoLevel = pgx.ReadUncommitted
	case ReadCommitted:
		pgxOpts.IsoLevel = pgx.ReadCommitted
	case RepeatableRead:
		pgxOpts.IsoLevel = pgx.RepeatableRead
	case Serializable:
		pgxOpts.IsoLevel = pgx.Serializable
	}

	switch opts.AccessMode {
	case ReadOnly:
		pgxOpts.AccessMode = pgx.ReadOnly
	case ReadWrite:
		pgxOpts.AccessMode = pgx.ReadWrite
	}

	return pgxOpts
}
