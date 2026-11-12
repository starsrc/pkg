package tx_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"

	"github.com/starsrc/pkg/tx"
)

func TestOptions(t *testing.T) {
	t.Parallel()

	t.Run("默认配置选项为空", func(t *testing.T) {
		t.Parallel()
		opts := tx.ApplyOptions()
		pgxOpts := tx.ToPGXTxOptions(opts)
		assert.Equal(t, pgx.TxOptions{}, pgxOpts)
	})

	t.Run("配置隔离级别", func(t *testing.T) {
		t.Parallel()
		tests := []struct {
			name     string
			input    tx.IsoLevel
			expected pgx.TxIsoLevel
		}{
			{
				name:     "ReadUncommitted",
				input:    tx.ReadUncommitted,
				expected: pgx.ReadUncommitted,
			},
			{
				name:     "ReadCommitted",
				input:    tx.ReadCommitted,
				expected: pgx.ReadCommitted,
			},
			{
				name:     "RepeatableRead",
				input:    tx.RepeatableRead,
				expected: pgx.RepeatableRead,
			},
			{
				name:     "Serializable",
				input:    tx.Serializable,
				expected: pgx.Serializable,
			},
			{
				name:     "未知隔离级别",
				input:    tx.IsoLevel("Unknown"),
				expected: pgx.TxIsoLevel(""),
			},
		}

		for _, tc := range tests {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				opts := tx.ApplyOptions(tx.WithIsoLevel(tc.input))
				assert.Equal(t, tc.input, opts.IsoLevel)
				pgxOpts := tx.ToPGXTxOptions(opts)
				assert.Equal(t, tc.expected, pgxOpts.IsoLevel)
			})
		}
	})

	t.Run("配置访问模式", func(t *testing.T) {
		t.Parallel()
		tests := []struct {
			name     string
			option   tx.Option
			expected pgx.TxAccessMode
		}{
			{
				name:     "ReadOnly",
				option:   tx.WithReadOnly(),
				expected: pgx.ReadOnly,
			},
			{
				name:     "ReadWrite",
				option:   tx.WithReadWrite(),
				expected: pgx.ReadWrite,
			},
		}

		for _, tc := range tests {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				opts := tx.ApplyOptions(tc.option)
				pgxOpts := tx.ToPGXTxOptions(opts)
				assert.Equal(t, tc.expected, pgxOpts.AccessMode)
			})
		}
	})

	t.Run("组合所有选项", func(t *testing.T) {
		t.Parallel()
		opts := tx.ApplyOptions(
			tx.WithIsoLevel(tx.Serializable),
			tx.WithReadOnly(),
		)
		pgxOpts := tx.ToPGXTxOptions(opts)
		assert.Equal(t, tx.Serializable, opts.IsoLevel)
		assert.Equal(t, tx.ReadOnly, opts.AccessMode)
		assert.Equal(t, pgx.Serializable, pgxOpts.IsoLevel)
		assert.Equal(t, pgx.ReadOnly, pgxOpts.AccessMode)
	})

	t.Run("配置自定义重试选项", func(t *testing.T) {
		t.Parallel()
		sentinel := errors.New("concurrent drift")
		opts := tx.ApplyOptions(
			tx.WithRetry(5, 10*time.Millisecond, 100*time.Millisecond, sentinel),
		)
		assert.Equal(t, 5, opts.MaxAttempts)
		assert.Equal(t, 10*time.Millisecond, opts.InitialBackoff)
		assert.Equal(t, 100*time.Millisecond, opts.MaxBackoff)
		assert.Equal(t, []error{sentinel}, opts.ExtraRetryable)
	})

	t.Run("配置自定义重试选项时自动修正 maxBackoff 小于 initialBackoff", func(t *testing.T) {
		t.Parallel()
		opts := tx.ApplyOptions(
			tx.WithRetry(3, 100*time.Millisecond, 20*time.Millisecond),
		)
		assert.Equal(t, 3, opts.MaxAttempts)
		assert.Equal(t, 100*time.Millisecond, opts.InitialBackoff)
		assert.Equal(t, 100*time.Millisecond, opts.MaxBackoff)
	})

	t.Run("配置默认重试选项", func(t *testing.T) {
		t.Parallel()
		sentinel := errors.New("concurrent drift")
		opts := tx.ApplyOptions(
			tx.WithDefaultRetry(sentinel),
		)
		assert.Equal(t, 16, opts.MaxAttempts)
		assert.Equal(t, 5*time.Millisecond, opts.InitialBackoff)
		assert.Equal(t, 50*time.Millisecond, opts.MaxBackoff)
		assert.Equal(t, []error{sentinel}, opts.ExtraRetryable)
	})
}

func TestIsRetryable(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("custom retryable sentinel")
	otherErr := errors.New("unrelated error")

	tests := []struct {
		name           string
		err            error
		extraRetryable []error
		expected       bool
	}{
		{
			name:     "nil 错误不可重试",
			err:      nil,
			expected: false,
		},
		{
			name:     "PostgreSQL 40001 (serialization_failure) 可重试",
			err:      &pgconn.PgError{Code: "40001"},
			expected: true,
		},
		{
			name:     "PostgreSQL 40P01 (deadlock_detected) 可重试",
			err:      &pgconn.PgError{Code: "40P01"},
			expected: true,
		},
		{
			name:     "被包装的 PostgreSQL 40001 可重试",
			err:      fmt.Errorf("execute query failed: %w", &pgconn.PgError{Code: "40001"}),
			expected: true,
		},
		{
			name:     "PostgreSQL 23505 (unique_violation) 不可重试",
			err:      &pgconn.PgError{Code: "23505"},
			expected: false,
		},
		{
			name:           "命中 extraRetryable 哨兵错误可重试",
			err:            sentinel,
			extraRetryable: []error{sentinel},
			expected:       true,
		},
		{
			name:           "被包装的 extraRetryable 哨兵错误可重试",
			err:            fmt.Errorf("wrapped: %w", sentinel),
			extraRetryable: []error{sentinel},
			expected:       true,
		},
		{
			name:           "extraRetryable 包含 nil 哨兵时安全忽略",
			err:            otherErr,
			extraRetryable: []error{nil},
			expected:       false,
		},
		{
			name:           "未命中 extraRetryable 的普通错误不可重试",
			err:            otherErr,
			extraRetryable: []error{sentinel},
			expected:       false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			actual := tx.IsRetryable(tc.err, tc.extraRetryable...)
			assert.Equal(t, tc.expected, actual)
		})
	}
}
