package tx_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/pkg/tx"
)

// mockTx 实现 pgx.Tx 接口，用于单元测试模拟底层事务行为。
type mockTx struct {
	commitErr           error
	rollbackErr         error
	committed           bool
	rolledBack          bool
	rollbackCtxErr      error
	rollbackHasDeadline bool
}

func (m *mockTx) Begin(ctx context.Context) (pgx.Tx, error) {
	return nil, errors.New("unimplemented")
}

func (m *mockTx) Commit(ctx context.Context) error {
	m.committed = true
	return m.commitErr
}

func (m *mockTx) Rollback(ctx context.Context) error {
	m.rolledBack = true
	m.rollbackCtxErr = ctx.Err()
	_, m.rollbackHasDeadline = ctx.Deadline()
	if err := ctx.Err(); err != nil {
		return err
	}
	return m.rollbackErr
}

func (m *mockTx) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return 0, errors.New("unimplemented")
}

func (m *mockTx) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return nil
}

func (m *mockTx) LargeObjects() pgx.LargeObjects {
	return pgx.LargeObjects{}
}

func (m *mockTx) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	return nil, errors.New("unimplemented")
}

func (m *mockTx) Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error) {
	return pgconn.CommandTag{}, errors.New("unimplemented")
}

func (m *mockTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return nil, errors.New("unimplemented")
}

func (m *mockTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return nil
}

func (m *mockTx) Conn() *pgx.Conn {
	return nil
}

// mockBeginner 模拟 tx.Beginner 接口。
type mockBeginner struct {
	beginTxErr   error
	tx           *mockTx
	recordedOpts pgx.TxOptions
}

func (m *mockBeginner) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	m.recordedOpts = txOptions
	if m.beginTxErr != nil {
		return nil, m.beginTxErr
	}
	return m.tx, nil
}

// multiTxBeginner 模拟多次事务调用的 tx.Beginner，每次 BeginTx 创建独立 mockTx 并记录。
type multiTxBeginner struct {
	txs          []*mockTx
	recordedOpts pgx.TxOptions
	beginErr     error
	onBegin      func(attempt int) (*mockTx, error)
}

func (m *multiTxBeginner) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	m.recordedOpts = txOptions
	if m.beginErr != nil {
		return nil, m.beginErr
	}
	attempt := len(m.txs)
	if m.onBegin != nil {
		tx, err := m.onBegin(attempt)
		if err != nil {
			return nil, err
		}
		m.txs = append(m.txs, tx)
		return tx, nil
	}
	tx := &mockTx{}
	m.txs = append(m.txs, tx)
	return tx, nil
}

// dummyQuerier 模拟微服务专属的 Querier 接口。
type dummyQuerier struct {
	tx pgx.Tx
}

func TestPGXTransactor_ExecTx(t *testing.T) {
	t.Run("默认选项提交成功", func(t *testing.T) {
		txStub := &mockTx{}
		beginner := &mockBeginner{tx: txStub}
		factory := func(rawTx pgx.Tx) *dummyQuerier {
			return &dummyQuerier{tx: rawTx}
		}

		transactor := tx.NewPGXTransactorWithBeginner(beginner, factory)

		var executedQuerier *dummyQuerier
		err := transactor.ExecTx(context.Background(), func(q *dummyQuerier) error {
			executedQuerier = q
			return nil
		})

		require.NoError(t, err)
		assert.True(t, txStub.committed)
		assert.False(t, txStub.rolledBack)
		assert.NotNil(t, executedQuerier)
		assert.Equal(t, txStub, executedQuerier.tx)
	})
}

func TestPGXTransactor_ExecTxWithOptions(t *testing.T) {
	t.Run("配置选项生效且提交成功", func(t *testing.T) {
		txStub := &mockTx{}
		beginner := &mockBeginner{tx: txStub}
		factory := func(rawTx pgx.Tx) *dummyQuerier {
			return &dummyQuerier{tx: rawTx}
		}

		transactor := tx.NewPGXTransactorWithBeginner(beginner, factory)

		err := transactor.ExecTxWithOptions(
			context.Background(),
			func(q *dummyQuerier) error {
				return nil
			},
			tx.WithIsoLevel(tx.Serializable),
			tx.WithReadOnly(),
		)

		require.NoError(t, err)
		assert.True(t, txStub.committed)
		assert.False(t, txStub.rolledBack)
		assert.Equal(t, pgx.Serializable, beginner.recordedOpts.IsoLevel)
		assert.Equal(t, pgx.ReadOnly, beginner.recordedOpts.AccessMode)
	})

	t.Run("BeginTx 发生错误", func(t *testing.T) {
		beginErr := errors.New("db connection pool exhausted")
		beginner := &mockBeginner{beginTxErr: beginErr}
		factory := func(rawTx pgx.Tx) *dummyQuerier {
			return &dummyQuerier{tx: rawTx}
		}

		transactor := tx.NewPGXTransactorWithBeginner(beginner, factory)

		called := false
		err := transactor.ExecTxWithOptions(context.Background(), func(q *dummyQuerier) error {
			called = true
			return nil
		})

		require.ErrorIs(t, err, beginErr)
		assert.False(t, called)
	})

	t.Run("业务逻辑执行失败触发回滚", func(t *testing.T) {
		txStub := &mockTx{}
		beginner := &mockBeginner{tx: txStub}
		factory := func(rawTx pgx.Tx) *dummyQuerier {
			return &dummyQuerier{tx: rawTx}
		}

		transactor := tx.NewPGXTransactorWithBeginner(beginner, factory)

		bizErr := errors.New("account balance insufficient")
		err := transactor.ExecTxWithOptions(context.Background(), func(q *dummyQuerier) error {
			return bizErr
		})

		require.ErrorIs(t, err, bizErr)
		assert.False(t, txStub.committed)
		assert.True(t, txStub.rolledBack)
	})

	t.Run("业务失败且回滚失败联合返回错误", func(t *testing.T) {
		rbErr := errors.New("rollback connection broken")
		txStub := &mockTx{rollbackErr: rbErr}
		beginner := &mockBeginner{tx: txStub}
		factory := func(rawTx pgx.Tx) *dummyQuerier {
			return &dummyQuerier{tx: rawTx}
		}

		transactor := tx.NewPGXTransactorWithBeginner(beginner, factory)

		bizErr := errors.New("business failed")
		err := transactor.ExecTxWithOptions(context.Background(), func(q *dummyQuerier) error {
			return bizErr
		})

		require.Error(t, err)
		assert.True(t, errors.Is(err, bizErr), "应包含业务错误")
		assert.True(t, errors.Is(err, rbErr), "应包含回滚错误")
		assert.False(t, txStub.committed)
		assert.True(t, txStub.rolledBack)
	})

	t.Run("业务失败且回滚报错 ErrTxClosed 时忽略关闭错误", func(t *testing.T) {
		txStub := &mockTx{rollbackErr: pgx.ErrTxClosed}
		beginner := &mockBeginner{tx: txStub}
		factory := func(rawTx pgx.Tx) *dummyQuerier {
			return &dummyQuerier{tx: rawTx}
		}

		transactor := tx.NewPGXTransactorWithBeginner(beginner, factory)

		bizErr := errors.New("business failed")
		err := transactor.ExecTxWithOptions(context.Background(), func(q *dummyQuerier) error {
			return bizErr
		})

		require.Equal(t, bizErr, err)
		assert.False(t, errors.Is(err, pgx.ErrTxClosed))
		assert.False(t, txStub.committed)
		assert.True(t, txStub.rolledBack)
	})

	t.Run("提交失败返回提交错误", func(t *testing.T) {
		commitErr := errors.New("serialization failure on commit")
		txStub := &mockTx{commitErr: commitErr}
		beginner := &mockBeginner{tx: txStub}
		factory := func(rawTx pgx.Tx) *dummyQuerier {
			return &dummyQuerier{tx: rawTx}
		}

		transactor := tx.NewPGXTransactorWithBeginner(beginner, factory)

		err := transactor.ExecTxWithOptions(context.Background(), func(q *dummyQuerier) error {
			return nil
		})

		require.ErrorIs(t, err, commitErr)
		assert.True(t, txStub.committed)
	})

	t.Run("业务逻辑触发 error Panic 时回滚并重新抛出", func(t *testing.T) {
		txStub := &mockTx{}
		beginner := &mockBeginner{tx: txStub}
		factory := func(rawTx pgx.Tx) *dummyQuerier {
			return &dummyQuerier{tx: rawTx}
		}

		transactor := tx.NewPGXTransactorWithBeginner(beginner, factory)

		panicErr := errors.New("unexpected nil pointer")
		assert.PanicsWithValue(t, panicErr, func() {
			_ = transactor.ExecTxWithOptions(context.Background(), func(q *dummyQuerier) error {
				panic(panicErr)
			})
		})

		assert.False(t, txStub.committed)
		assert.True(t, txStub.rolledBack)
	})

	t.Run("业务逻辑触发非 error Panic 时回滚并重新抛出", func(t *testing.T) {
		txStub := &mockTx{}
		beginner := &mockBeginner{tx: txStub}
		factory := func(rawTx pgx.Tx) *dummyQuerier {
			return &dummyQuerier{tx: rawTx}
		}

		transactor := tx.NewPGXTransactorWithBeginner(beginner, factory)

		panicVal := "critical invariant violated"
		assert.PanicsWithValue(t, panicVal, func() {
			_ = transactor.ExecTxWithOptions(context.Background(), func(q *dummyQuerier) error {
				panic(panicVal)
			})
		})

		assert.False(t, txStub.committed)
		assert.True(t, txStub.rolledBack)
	})

	t.Run("工厂闭包触发 Panic 时已开启事务回滚并重新抛出", func(t *testing.T) {
		txStub := &mockTx{}
		beginner := &mockBeginner{tx: txStub}
		factoryErr := errors.New("factory failure")
		factory := func(rawTx pgx.Tx) *dummyQuerier {
			panic(factoryErr)
		}

		transactor := tx.NewPGXTransactorWithBeginner(beginner, factory)

		assert.PanicsWithValue(t, factoryErr, func() {
			_ = transactor.ExecTxWithOptions(context.Background(), func(q *dummyQuerier) error {
				return nil
			})
		})

		assert.False(t, txStub.committed)
		assert.True(t, txStub.rolledBack, "工厂闭包 panic 时底层事务必须回滚以防连接泄漏")
	})

	t.Run("工厂闭包为 nil 触发 Panic 时已开启事务回滚并重新抛出", func(t *testing.T) {
		txStub := &mockTx{}
		beginner := &mockBeginner{tx: txStub}

		transactor := tx.NewPGXTransactorWithBeginner[*dummyQuerier](beginner, nil)

		assert.Panics(t, func() {
			_ = transactor.ExecTxWithOptions(context.Background(), func(q *dummyQuerier) error {
				return nil
			})
		})

		assert.False(t, txStub.committed)
		assert.True(t, txStub.rolledBack, "工厂闭包为 nil 时底层事务必须回滚以防连接泄漏")
	})

	t.Run("外层 context 已取消时事务仍可成功安全回滚", func(t *testing.T) {
		txStub := &mockTx{}
		beginner := &mockBeginner{tx: txStub}
		factory := func(rawTx pgx.Tx) *dummyQuerier {
			return &dummyQuerier{tx: rawTx}
		}

		transactor := tx.NewPGXTransactorWithBeginner(beginner, factory)

		ctx, cancel := context.WithCancel(context.Background())

		err := transactor.ExecTxWithOptions(ctx, func(q *dummyQuerier) error {
			cancel() // 模拟业务执行期间上下文被取消或超时
			return ctx.Err()
		})

		require.ErrorIs(t, err, context.Canceled)
		assert.False(t, txStub.committed)
		assert.True(t, txStub.rolledBack)
		assert.NoError(t, txStub.rollbackCtxErr, "回滚上下文必须解耦外层取消信号")
		assert.True(t, txStub.rollbackHasDeadline, "回滚上下文必须配置超时保护")
	})

	t.Run("外层 context 已取消且触发 Panic 时仍使用安全上下文回滚", func(t *testing.T) {
		txStub := &mockTx{}
		beginner := &mockBeginner{tx: txStub}
		factory := func(rawTx pgx.Tx) *dummyQuerier {
			return &dummyQuerier{tx: rawTx}
		}

		transactor := tx.NewPGXTransactorWithBeginner(beginner, factory)

		ctx, cancel := context.WithCancel(context.Background())

		panicErr := errors.New("unexpected panic during canceled transaction")
		assert.PanicsWithValue(t, panicErr, func() {
			_ = transactor.ExecTxWithOptions(ctx, func(q *dummyQuerier) error {
				cancel() // 模拟取消后发生 panic
				panic(panicErr)
			})
		})

		assert.False(t, txStub.committed)
		assert.True(t, txStub.rolledBack)
		assert.NoError(t, txStub.rollbackCtxErr, "回滚上下文必须解耦外层取消信号")
		assert.True(t, txStub.rollbackHasDeadline, "回滚上下文必须配置超时保护")
	})

	t.Run("默认无重试选项时遇到可重试错误只执行单次并返回原错", func(t *testing.T) {
		beginner := &multiTxBeginner{}
		factory := func(rawTx pgx.Tx) *dummyQuerier {
			return &dummyQuerier{tx: rawTx}
		}
		transactor := tx.NewPGXTransactorWithBeginner(beginner, factory)

		pgErr := &pgconn.PgError{Code: "40001"}
		calls := 0
		err := transactor.ExecTxWithOptions(context.Background(), func(q *dummyQuerier) error {
			calls++
			return pgErr
		})

		require.ErrorIs(t, err, pgErr)
		assert.Equal(t, 1, calls)
		require.Len(t, beginner.txs, 1)
		assert.True(t, beginner.txs[0].rolledBack)
		assert.False(t, beginner.txs[0].committed)
	})

	t.Run("配置 MaxAttempts 为 1 时不执行重试", func(t *testing.T) {
		beginner := &multiTxBeginner{}
		factory := func(rawTx pgx.Tx) *dummyQuerier {
			return &dummyQuerier{tx: rawTx}
		}
		transactor := tx.NewPGXTransactorWithBeginner(beginner, factory)

		pgErr := &pgconn.PgError{Code: "40001"}
		calls := 0
		err := transactor.ExecTxWithOptions(
			context.Background(),
			func(q *dummyQuerier) error {
				calls++
				return pgErr
			},
			tx.WithRetry(1, time.Millisecond, 5*time.Millisecond),
		)

		require.ErrorIs(t, err, pgErr)
		assert.Equal(t, 1, calls)
		require.Len(t, beginner.txs, 1)
		assert.True(t, beginner.txs[0].rolledBack)
	})

	t.Run("重试成功恢复 40001 串行化冲突", func(t *testing.T) {
		beginner := &multiTxBeginner{}
		factory := func(rawTx pgx.Tx) *dummyQuerier {
			return &dummyQuerier{tx: rawTx}
		}
		transactor := tx.NewPGXTransactorWithBeginner(beginner, factory)

		pgErr := &pgconn.PgError{Code: "40001"}
		calls := 0
		err := transactor.ExecTxWithOptions(
			context.Background(),
			func(q *dummyQuerier) error {
				calls++
				if calls < 3 {
					return pgErr
				}
				return nil
			},
			tx.WithRetry(5, time.Millisecond, 5*time.Millisecond),
		)

		require.NoError(t, err)
		assert.Equal(t, 3, calls)
		require.Len(t, beginner.txs, 3)

		// 前两次失败的事务干净回滚
		assert.True(t, beginner.txs[0].rolledBack)
		assert.False(t, beginner.txs[0].committed)
		assert.True(t, beginner.txs[1].rolledBack)
		assert.False(t, beginner.txs[1].committed)

		// 第三次成功的事务成功提交
		assert.False(t, beginner.txs[2].rolledBack)
		assert.True(t, beginner.txs[2].committed)
	})

	t.Run("重试成功恢复 40P01 死锁检测", func(t *testing.T) {
		beginner := &multiTxBeginner{}
		factory := func(rawTx pgx.Tx) *dummyQuerier {
			return &dummyQuerier{tx: rawTx}
		}
		transactor := tx.NewPGXTransactorWithBeginner(beginner, factory)

		pgErr := &pgconn.PgError{Code: "40P01"}
		calls := 0
		err := transactor.ExecTxWithOptions(
			context.Background(),
			func(q *dummyQuerier) error {
				calls++
				if calls == 1 {
					return pgErr
				}
				return nil
			},
			tx.WithRetry(3, time.Millisecond, 5*time.Millisecond),
		)

		require.NoError(t, err)
		assert.Equal(t, 2, calls)
		require.Len(t, beginner.txs, 2)
		assert.True(t, beginner.txs[0].rolledBack)
		assert.True(t, beginner.txs[1].committed)
	})

	t.Run("重试成功恢复 extraRetryable 自定义哨兵错误", func(t *testing.T) {
		beginner := &multiTxBeginner{}
		factory := func(rawTx pgx.Tx) *dummyQuerier {
			return &dummyQuerier{tx: rawTx}
		}
		transactor := tx.NewPGXTransactorWithBeginner(beginner, factory)

		sentinel := errors.New("concurrent state drift")
		calls := 0
		err := transactor.ExecTxWithOptions(
			context.Background(),
			func(q *dummyQuerier) error {
				calls++
				if calls == 1 {
					return sentinel
				}
				return nil
			},
			tx.WithDefaultRetry(sentinel),
		)

		require.NoError(t, err)
		assert.Equal(t, 2, calls)
		require.Len(t, beginner.txs, 2)
		assert.True(t, beginner.txs[0].rolledBack)
		assert.True(t, beginner.txs[1].committed)
	})

	t.Run("非可重试错误立即中止且干净回滚当前事务", func(t *testing.T) {
		beginner := &multiTxBeginner{}
		factory := func(rawTx pgx.Tx) *dummyQuerier {
			return &dummyQuerier{tx: rawTx}
		}
		transactor := tx.NewPGXTransactorWithBeginner(beginner, factory)

		bizErr := errors.New("invalid parameter")
		calls := 0
		err := transactor.ExecTxWithOptions(
			context.Background(),
			func(q *dummyQuerier) error {
				calls++
				return bizErr
			},
			tx.WithRetry(5, 50*time.Millisecond, 200*time.Millisecond),
		)

		require.ErrorIs(t, err, bizErr)
		assert.Equal(t, 1, calls)
		require.Len(t, beginner.txs, 1)
		assert.True(t, beginner.txs[0].rolledBack)
		assert.False(t, beginner.txs[0].committed)
	})

	t.Run("耗尽重试次数后返回格式化错误并干净回滚每次事务", func(t *testing.T) {
		beginner := &multiTxBeginner{}
		factory := func(rawTx pgx.Tx) *dummyQuerier {
			return &dummyQuerier{tx: rawTx}
		}
		transactor := tx.NewPGXTransactorWithBeginner(beginner, factory)

		pgErr := &pgconn.PgError{Code: "40001"}
		calls := 0
		err := transactor.ExecTxWithOptions(
			context.Background(),
			func(q *dummyQuerier) error {
				calls++
				return pgErr
			},
			tx.WithRetry(3, time.Millisecond, 5*time.Millisecond),
		)

		require.Error(t, err)
		assert.True(t, errors.Is(err, pgErr), "应支持 errors.Is 展开匹配原始底层错误")
		assert.Contains(t, err.Error(), "exceeded max transaction retries (3):")
		assert.Equal(t, 3, calls)
		require.Len(t, beginner.txs, 3)
		for i, txObj := range beginner.txs {
			assert.Truef(t, txObj.rolledBack, "第 %d 次事务必须已回滚", i+1)
			assert.Falsef(t, txObj.committed, "第 %d 次事务绝不能提交", i+1)
		}
	})

	t.Run("重试休眠期间上下文取消立即中断重试并回滚", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			beginner := &multiTxBeginner{}
			factory := func(rawTx pgx.Tx) *dummyQuerier {
				return &dummyQuerier{tx: rawTx}
			}
			transactor := tx.NewPGXTransactorWithBeginner(beginner, factory)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			pgErr := &pgconn.PgError{Code: "40001"}
			calls := 0
			firstAttemptDone := make(chan struct{})

			errCh := make(chan error, 1)
			start := time.Now()

			go func() {
				errCh <- transactor.ExecTxWithOptions(
					ctx,
					func(q *dummyQuerier) error {
						calls++
						if calls == 1 {
							close(firstAttemptDone)
						}
						return pgErr
					},
					tx.WithRetry(10, 2*time.Second, 5*time.Second),
				)
			}()

			<-firstAttemptDone
			// 确保被测协程已越过首次回滚、越过 retry.go:33 并稳定阻塞在 retry.go:47 的退避 select 上
			synctest.Wait()

			assert.Equal(t, 1, calls)
			require.Len(t, beginner.txs, 1)
			assert.True(t, beginner.txs[0].rolledBack)

			cancel()

			err := <-errCh
			elapsed := time.Since(start)

			require.ErrorIs(t, err, context.Canceled)
			assert.Equal(t, 1, calls)
			assert.Equal(t, time.Duration(0), elapsed, "收到取消信号后应立即中断退出，虚拟时钟耗时应为 0")
		})
	})

	t.Run("提交阶段触发 40001 串行化冲突被重试并在下次成功提交", func(t *testing.T) {
		commitPgErr := &pgconn.PgError{Code: "40001"}
		beginner := &multiTxBeginner{
			onBegin: func(attempt int) (*mockTx, error) {
				if attempt == 0 {
					return &mockTx{commitErr: commitPgErr}, nil
				}
				return &mockTx{}, nil
			},
		}
		factory := func(rawTx pgx.Tx) *dummyQuerier {
			return &dummyQuerier{tx: rawTx}
		}
		transactor := tx.NewPGXTransactorWithBeginner(beginner, factory)

		calls := 0
		err := transactor.ExecTxWithOptions(
			context.Background(),
			func(q *dummyQuerier) error {
				calls++
				return nil
			},
			tx.WithRetry(3, time.Millisecond, 5*time.Millisecond),
		)

		require.NoError(t, err)
		assert.Equal(t, 2, calls)
		require.Len(t, beginner.txs, 2)
		assert.True(t, beginner.txs[0].committed)
		assert.True(t, beginner.txs[1].committed)
	})
}

func TestNewPGXTransactor(t *testing.T) {
	t.Parallel()

	t.Run("binds pool and factory for transaction execution", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)

		serverDone := make(chan struct{})
		go func() {
			defer close(serverDone)
			conn, acceptErr := ln.Accept()
			if acceptErr != nil {
				return
			}
			defer conn.Close()

			buf := make([]byte, 1024)
			// Read StartupMessage
			if _, readErr := conn.Read(buf); readErr != nil {
				return
			}
			// Send AuthenticationOk ('R', len=8, status=0)
			if _, writeErr := conn.Write([]byte{'R', 0, 0, 0, 8, 0, 0, 0, 0}); writeErr != nil {
				return
			}
			// Send ReadyForQuery ('Z', len=5, status='I')
			if _, writeErr := conn.Write([]byte{'Z', 0, 0, 0, 5, 'I'}); writeErr != nil {
				return
			}

			for {
				n, readErr := conn.Read(buf)
				if readErr != nil || n == 0 {
					return
				}
				msgType := buf[0]
				switch msgType {
				case 'Q': // Simple Query (BEGIN / COMMIT)
					tag := "OK\x00"
					cmdLen := int32(4 + len(tag))
					res := []byte{'C', byte(cmdLen >> 24), byte(cmdLen >> 16), byte(cmdLen >> 8), byte(cmdLen)}
					res = append(res, []byte(tag)...)
					res = append(res, 'Z', 0, 0, 0, 5, 'T')
					if _, writeErr := conn.Write(res); writeErr != nil {
						return
					}
				case 'X': // Terminate
					return
				default:
					if _, writeErr := conn.Write([]byte{'Z', 0, 0, 0, 5, 'I'}); writeErr != nil {
						return
					}
				}
			}
		}()

		pool, err := pgxpool.New(context.Background(), "postgres://user:pass@"+ln.Addr().String()+"/testdb?sslmode=disable")
		require.NoError(t, err)

		factoryCalled := false
		var receivedTx pgx.Tx
		factory := func(rawTx pgx.Tx) *dummyQuerier {
			factoryCalled = true
			receivedTx = rawTx
			return &dummyQuerier{tx: rawTx}
		}

		transactor := tx.NewPGXTransactor(pool, factory)
		require.NotNil(t, transactor)

		var querierInCallback *dummyQuerier
		err = transactor.ExecTx(context.Background(), func(q *dummyQuerier) error {
			querierInCallback = q
			return nil
		})
		require.NoError(t, err)

		assert.True(t, factoryCalled, "factory should be invoked")
		require.NotNil(t, querierInCallback, "querier should be delivered to callback")
		assert.NotNil(t, receivedTx, "underlying pgx.Tx should be provided to factory")
		assert.Equal(t, receivedTx, querierInCallback.tx)

		pool.Close()
		_ = ln.Close()
		<-serverDone
	})

	t.Run("binds pool which propagates closed pool error", func(t *testing.T) {
		pool, err := pgxpool.New(context.Background(), "postgres://user:pass@127.0.0.1:5432/testdb?pool_min_conns=0")
		require.NoError(t, err)
		pool.Close()

		transactor := tx.NewPGXTransactor(pool, func(rawTx pgx.Tx) *dummyQuerier {
			return &dummyQuerier{tx: rawTx}
		})
		require.NotNil(t, transactor)

		err = transactor.ExecTx(context.Background(), func(q *dummyQuerier) error {
			return nil
		})
		require.Error(t, err)
		assert.ErrorContains(t, err, "closed pool")
	})
}
