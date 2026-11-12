package tx

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultRollbackTimeout = 5 * time.Second
)

var _ Transactor[any] = (*PGXTransactor[any])(nil)

// PGXTransactor 基于 pgx 提供通用的泛型事务执行器实现。
type PGXTransactor[Q any] struct {
	beginner Beginner
	factory  FactoryFunc[Q]
}

// NewPGXTransactor 构造基于连接池的事务执行器实例。
func NewPGXTransactor[Q any](pool *pgxpool.Pool, factory FactoryFunc[Q]) *PGXTransactor[Q] {
	return NewPGXTransactorWithBeginner[Q](pool, factory)
}

// NewPGXTransactorWithBeginner 允许注入自定义 Beginner（例如用于单元测试桩）。
func NewPGXTransactorWithBeginner[Q any](beginner Beginner, factory FactoryFunc[Q]) *PGXTransactor[Q] {
	return &PGXTransactor[Q]{
		beginner: beginner,
		factory:  factory,
	}
}

// ExecTx 使用默认事务选项执行原子事务操作。
func (t *PGXTransactor[Q]) ExecTx(ctx context.Context, fn func(Q) error) error {
	return t.ExecTxWithOptions(ctx, fn)
}

// ExecTxWithOptions 使用指定配置选项执行原子事务操作。
// 若未配置重试策略（或 MaxAttempts <= 1），则直接执行单次事务尝试；
// 当启用重试策略时，对 PostgreSQL 40001 (serialization_failure)、40P01 (deadlock_detected)
// 以及 extraRetryable 哨兵错误执行指数回退重试，每次重试前均干净回滚前序事务，
// 并在重试次数耗尽后返回包装错误。
func (t *PGXTransactor[Q]) ExecTxWithOptions(ctx context.Context, fn func(Q) error, opts ...Option) error {
	options := ApplyOptions(opts...)
	pgxOpts := ToPGXTxOptions(options)

	if options.MaxAttempts <= 1 {
		return t.execSingleTx(ctx, fn, pgxOpts)
	}

	return Retry(ctx, func(attemptCtx context.Context) error {
		return t.execSingleTx(attemptCtx, fn, pgxOpts)
	}, opts...)
}

func (t *PGXTransactor[Q]) execSingleTx(ctx context.Context, fn func(Q) error, pgxOpts pgx.TxOptions) (txErr error) {
	tx, err := t.beginner.BeginTx(ctx, pgxOpts)
	if err != nil {
		return err
	}

	defer func() {
		if p := recover(); p != nil {
			_ = rollbackTx(ctx, tx)
			panic(p)
		}

		if txErr != nil {
			if rbErr := rollbackTx(ctx, tx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
				txErr = errors.Join(txErr, rbErr)
			}
		} else {
			if commitErr := tx.Commit(ctx); commitErr != nil {
				txErr = commitErr
			}
		}
	}()

	q := t.factory(tx)
	txErr = fn(q)
	return txErr
}

// rollbackTx 在解耦取消信号的独立上下文下执行事务回滚，并配置 5 秒超时保护。
func rollbackTx(ctx context.Context, tx pgx.Tx) error {
	rollbackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultRollbackTimeout)
	defer cancel()
	return tx.Rollback(rollbackCtx)
}
