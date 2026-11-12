package tx_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/starsrc/pkg/tx"
)

func TestRetry(t *testing.T) {
	t.Parallel()

	t.Run("默认单次尝试成功", func(t *testing.T) {
		t.Parallel()
		calls := 0
		err := tx.Retry(context.Background(), func(ctx context.Context) error {
			calls++
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 1, calls)
	})

	t.Run("默认单次尝试失败直接返回", func(t *testing.T) {
		t.Parallel()
		calls := 0
		targetErr := errors.New("normal error")
		err := tx.Retry(context.Background(), func(ctx context.Context) error {
			calls++
			return targetErr
		})
		require.ErrorIs(t, err, targetErr)
		assert.Equal(t, 1, calls)
	})

	t.Run("非可重试错误立即中止且不进行重试休眠", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			calls := 0
			nonRetryable := errors.New("fatal business error")

			start := time.Now()
			err := tx.Retry(
				context.Background(),
				func(ctx context.Context) error {
					calls++
					return nonRetryable
				},
				tx.WithRetry(5, 50*time.Millisecond, 200*time.Millisecond),
			)
			duration := time.Since(start)

			require.ErrorIs(t, err, nonRetryable)
			assert.Equal(t, 1, calls)
			assert.Equal(t, time.Duration(0), duration, "非可重试错误不应产生回退休眠等待")
		})
	})

	t.Run("可重试错误在重试后成功恢复", func(t *testing.T) {
		t.Parallel()
		calls := 0
		pgErr := &pgconn.PgError{Code: "40001"}

		err := tx.Retry(
			context.Background(),
			func(ctx context.Context) error {
				calls++
				if calls < 3 {
					return pgErr
				}
				return nil
			},
			tx.WithRetry(5, time.Millisecond, 10*time.Millisecond),
		)

		require.NoError(t, err)
		assert.Equal(t, 3, calls)
	})

	t.Run("耗尽重试次数后返回包装错误", func(t *testing.T) {
		t.Parallel()
		calls := 0
		pgErr := &pgconn.PgError{
			Severity: "ERROR",
			Code:     "40P01",
			Message:  "deadlock detected",
		}

		err := tx.Retry(
			context.Background(),
			func(ctx context.Context) error {
				calls++
				return pgErr
			},
			tx.WithRetry(3, time.Millisecond, 5*time.Millisecond),
		)

		require.Error(t, err)
		assert.True(t, errors.Is(err, pgErr), "返回的错误应通过 errors.Is 展开匹配原始错误")
		assert.Equal(t, "exceeded max transaction retries (3): ERROR: deadlock detected (SQLSTATE 40P01)", err.Error())
		assert.Equal(t, 3, calls)
	})

	t.Run("重试休眠期间上下文取消立即中断", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := int32(0)
			pgErr := &pgconn.PgError{Code: "40001"}
			firstAttemptDone := make(chan struct{})

			errCh := make(chan error, 1)
			start := time.Now()

			go func() {
				errCh <- tx.Retry(
					ctx,
					func(attemptCtx context.Context) error {
						if atomic.AddInt32(&calls, 1) == 1 {
							close(firstAttemptDone)
						}
						return pgErr
					},
					tx.WithRetry(10, 2*time.Second, 5*time.Second),
				)
			}()

			<-firstAttemptDone
			// 确保被测协程已越过 retry.go:33 并稳定阻塞在 retry.go:47 的退避 select 上
			synctest.Wait()

			// 此时被测协程稳定阻塞在 select 上，执行取消
			cancel()

			err := <-errCh
			elapsed := time.Since(start)

			require.ErrorIs(t, err, context.Canceled)
			assert.Equal(t, int32(1), atomic.LoadInt32(&calls), "应在第一次休眠期间即刻退出")
			assert.Equal(t, time.Duration(0), elapsed, "收到取消信号后应立即中断退出，虚拟时钟耗时应为 0")
		})
	})

	t.Run("执行前上下文已过期直接返回错误", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		calls := 0
		err := tx.Retry(
			ctx,
			func(attemptCtx context.Context) error {
				calls++
				return nil
			},
			tx.WithRetry(3, time.Millisecond, 5*time.Millisecond),
		)

		require.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, 0, calls)
	})

	t.Run("回退延迟按指数翻倍且受限于 maxBackoff", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			calls := 0
			pgErr := &pgconn.PgError{Code: "40001"}
			var sleepIntervals []time.Duration
			lastCallTime := time.Time{}

			err := tx.Retry(
				context.Background(),
				func(ctx context.Context) error {
					now := time.Now()
					if !lastCallTime.IsZero() {
						sleepIntervals = append(sleepIntervals, now.Sub(lastCallTime))
					}
					lastCallTime = now
					calls++
					return pgErr
				},
				tx.WithRetry(6, 10*time.Millisecond, 50*time.Millisecond),
			)

			require.Error(t, err)
			assert.Equal(t, 6, calls)
			require.Len(t, sleepIntervals, 5)

			assert.Equal(t, 10*time.Millisecond, sleepIntervals[0], "第1次退避应为 10ms")
			assert.Equal(t, 20*time.Millisecond, sleepIntervals[1], "第2次退避翻倍至 20ms")
			assert.Equal(t, 40*time.Millisecond, sleepIntervals[2], "第3次退避翻倍至 40ms")
			assert.Equal(t, 50*time.Millisecond, sleepIntervals[3], "第4次退避受限于 maxBackoff 截断为 50ms")
			assert.Equal(t, 50*time.Millisecond, sleepIntervals[4], "第5次退避受限于 maxBackoff 截断为 50ms")
		})
	})
}

func TestRetry_MetricsAndSpanEvents(t *testing.T) {
	origMP := otel.GetMeterProvider()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(origMP)
		_ = mp.Shutdown(context.Background())
	})

	spanRecorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	ctx, span := tp.Tracer("tx-test").Start(context.Background(), "run-tx")

	// 1. 40001 重试 2 次后第 3 次成功（retries=2, exhausted=0）
	calls := 0
	err := tx.Retry(
		ctx,
		func(context.Context) error {
			calls++
			if calls <= 2 {
				return &pgconn.PgError{Code: tx.SQLStateSerializationFailure}
			}
			return nil
		},
		tx.WithRetry(4, 0, 0),
	)
	require.NoError(t, err)

	// 2. 40P01 重试耗尽 MaxAttempts=3（retries=2, exhausted=1）
	err = tx.Retry(
		ctx,
		func(context.Context) error {
			return &pgconn.PgError{Code: tx.SQLStateDeadlockDetected, Message: "deadlock"}
		},
		tx.WithRetry(3, 0, 0),
	)
	require.Error(t, err)

	// 3. 自定义哨兵重试 1 次后成功（sqlstate="custom", retries=1）
	sentinelErr := errors.New("optimistic epoch conflict")
	customCalls := 0
	err = tx.Retry(
		ctx,
		func(context.Context) error {
			customCalls++
			if customCalls == 1 {
				return sentinelErr
			}
			return nil
		},
		tx.WithRetry(3, 0, 0, sentinelErr),
	)
	require.NoError(t, err)

	span.End()

	// 验证 Span Events
	ended := spanRecorder.Ended()
	require.Len(t, ended, 1)
	var retryEvents int
	for _, ev := range ended[0].Events() {
		if ev.Name == "db.tx.retry" {
			retryEvents++
		}
	}
	assert.Equal(t, 5, retryEvents, "活跃录制 Span 上应记录全部 5 次 db.tx.retry 事件")

	// 验证 OTel Counter 数值与标签
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	counterVals := make(map[string]map[string]int64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			if counterVals[m.Name] == nil {
				counterVals[m.Name] = make(map[string]int64)
			}
			for _, dp := range sum.DataPoints {
				sysVal, hasSys := dp.Attributes.Value(attribute.Key("db.system.name"))
				require.True(t, hasSys)
				assert.Equal(t, "postgresql", sysVal.AsString())

				sqlVal, hasSQL := dp.Attributes.Value(attribute.Key("sqlstate"))
				require.True(t, hasSQL)
				counterVals[m.Name][sqlVal.AsString()] = dp.Value
			}
		}
	}

	assert.Equal(t, int64(2), counterVals["db.client.tx.retries"]["40001"])
	assert.Equal(t, int64(2), counterVals["db.client.tx.retries"]["40P01"])
	assert.Equal(t, int64(1), counterVals["db.client.tx.retries"]["custom"])
	assert.Equal(t, int64(1), counterVals["db.client.tx.retry_exhausted"]["40P01"])
	assert.Zero(t, counterVals["db.client.tx.retry_exhausted"]["40001"])
}
