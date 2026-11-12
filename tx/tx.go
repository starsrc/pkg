package tx

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// Transactor 定义通用事务执行器接口。
// 类型参数 Q 代表微服务业务绑定的查询器接口类型（例如 Querier）。
type Transactor[Q any] interface {
	// ExecTx 使用默认事务选项执行原子事务操作。
	ExecTx(ctx context.Context, fn func(Q) error) error
	// ExecTxWithOptions 使用指定配置选项执行原子事务操作。
	ExecTxWithOptions(ctx context.Context, fn func(Q) error, opts ...Option) error
}

// FactoryFunc 定义通过底层 pgx.Tx 实例构造类型安全 Querier 的工厂闭包类型。
type FactoryFunc[Q any] func(pgx.Tx) Q

// Beginner 定义底层事务发起者抽象接口，兼容 *pgxpool.Pool 与测试桩。
type Beginner interface {
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
}
