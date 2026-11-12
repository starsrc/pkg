package tx

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const (
	txInstrumentationName = "github.com/starsrc/pkg/tx"
	metricTxRetries       = "db.client.tx.retries"
	metricTxExhausted     = "db.client.tx.retry_exhausted"
	sqlStateCustom        = "custom"
)

var (
	attrDBSystem  = attribute.String("db.system.name", "postgresql")
	retryOpt40001 = metric.WithAttributes(attrDBSystem, attribute.String("sqlstate", SQLStateSerializationFailure))
	retryOpt40P01 = metric.WithAttributes(attrDBSystem, attribute.String("sqlstate", SQLStateDeadlockDetected))
	retryOptOther = metric.WithAttributes(attrDBSystem, attribute.String("sqlstate", sqlStateCustom))

	cachedRetryInst atomic.Pointer[retryInstruments]
)

type retryInstruments struct {
	mp         metric.MeterProvider
	comparable bool
	retries    metric.Int64Counter
	exhausted  metric.Int64Counter
}

func getRetryInstruments() *retryInstruments {
	mp := otel.GetMeterProvider()
	if cur := cachedRetryInst.Load(); cur != nil && cur.comparable && cur.mp == mp {
		return cur
	}

	meter := mp.Meter(txInstrumentationName)
	retriesCounter, _ := meter.Int64Counter(
		metricTxRetries,
		metric.WithDescription("Number of transient database transaction retry attempts"),
	)
	exhaustedCounter, _ := meter.Int64Counter(
		metricTxExhausted,
		metric.WithDescription("Number of database transactions that exhausted all retry attempts"),
	)

	isComp := mp != nil && reflect.TypeOf(mp).Comparable()
	inst := &retryInstruments{
		mp:         mp,
		comparable: isComp,
		retries:    retriesCounter,
		exhausted:  exhaustedCounter,
	}
	if isComp {
		cachedRetryInst.Store(inst)
	}
	return inst
}

func retryMetricOption(sqlState string) metric.MeasurementOption {
	switch sqlState {
	case SQLStateSerializationFailure:
		return retryOpt40001
	case SQLStateDeadlockDetected:
		return retryOpt40P01
	default:
		return retryOptOther
	}
}

func classifyRetryable(err error, extraRetryable ...error) (string, bool) {
	if err == nil {
		return "", false
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if pgErr.Code == SQLStateSerializationFailure || pgErr.Code == SQLStateDeadlockDetected {
			return pgErr.Code, true
		}
	}
	for _, target := range extraRetryable {
		if target != nil && errors.Is(err, target) {
			return sqlStateCustom, true
		}
	}
	return "", false
}

// Retry 使用指定的重试策略对闭包执行重试调用。
// 若未配置重试策略（或 MaxAttempts <= 1），则直接单次执行并返回结果。
// 当启用重试时，仅在遇到 IsRetryable 认定的暂时性错误时执行指数回退重试，
// 遇到不可重试错误或上下文取消时立即中断返回。
// 当尝试次数耗尽后，返回格式为 "exceeded max transaction retries (%d): %w" 的错误。
func Retry(ctx context.Context, fn func(ctx context.Context) error, opts ...Option) error {
	options := ApplyOptions(opts...)
	if options.MaxAttempts <= 1 {
		return fn(ctx)
	}

	var (
		lastErr      error
		lastSQLState string
	)
	backoff := options.InitialBackoff

	for attempt := 0; attempt < options.MaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := fn(ctx)
		if err == nil {
			return nil
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		sqlState, retryable := classifyRetryable(err, options.ExtraRetryable...)
		if !retryable {
			return err
		}

		lastErr = err
		lastSQLState = sqlState
		if attempt == options.MaxAttempts-1 {
			break
		}

		inst := getRetryInstruments()
		if inst.retries != nil {
			inst.retries.Add(ctx, 1, retryMetricOption(sqlState))
		}
		if span := trace.SpanFromContext(ctx); span.IsRecording() {
			span.AddEvent("db.tx.retry", trace.WithAttributes(
				attribute.Int("attempt", attempt+1),
				attribute.String("sqlstate", sqlState),
			))
		}

		if backoff > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
			if options.MaxBackoff > 0 && backoff > options.MaxBackoff {
				backoff = options.MaxBackoff
			}
		} else {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
	}

	inst := getRetryInstruments()
	if inst.exhausted != nil {
		inst.exhausted.Add(ctx, 1, retryMetricOption(lastSQLState))
	}

	return fmt.Errorf("exceeded max transaction retries (%d): %w", options.MaxAttempts, lastErr)
}
