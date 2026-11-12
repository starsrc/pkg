package slogx_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/starsrc/pkg/observability/slogx"
)

// mockRequest 实现 connect.AnyRequest 用于测试
type mockRequest struct {
	connect.AnyRequest
	procedure string
	header    http.Header
}

func (m *mockRequest) Spec() connect.Spec  { return connect.Spec{Procedure: m.procedure} }
func (m *mockRequest) Header() http.Header { return m.header }

func newTestRequest(procedure string) connect.AnyRequest {
	return &mockRequest{
		AnyRequest: connect.NewRequest(&emptypb.Empty{}),
		procedure:  procedure,
		header:     make(http.Header),
	}
}

func TestNewConnectInterceptor(t *testing.T) {
	t.Run("successful RPC call logs with ok code and exact duration using synctest", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

			interceptor := slogx.NewConnectInterceptor(nil, logger)
			const sleepDuration = 25 * time.Millisecond
			handler := interceptor(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
				time.Sleep(sleepDuration)
				return connect.NewResponse(&emptypb.Empty{}), nil
			})

			req := newTestRequest("/aether.v1.AuthService/Login")
			res, err := handler(context.Background(), req)
			require.NoError(t, err)
			assert.NotNil(t, res)

			var logMap map[string]any
			require.NoError(t, json.Unmarshal(buf.Bytes(), &logMap))
			assert.Equal(t, "/aether.v1.AuthService/Login", logMap[slogx.DefaultProcedureKey])
			assert.Equal(t, "ok", logMap[slogx.DefaultCodeKey])
			assert.Equal(t, float64(sleepDuration.Nanoseconds()), logMap[slogx.DefaultDurationKey], "duration必须精确等于虚拟休眠时间对应的纳秒数值，杜绝常量0假绿")
			assert.Equal(t, slogx.DefaultConnectMessage, logMap["msg"])
		})
	})

	t.Run("configured non-empty Message overrides DefaultConnectMessage", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

		cfg := &mockLogConfig{
			message: "custom rpc completed",
		}
		interceptor := slogx.NewConnectInterceptor(cfg, logger)
		handler := interceptor(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			return connect.NewResponse(&emptypb.Empty{}), nil
		})

		req := newTestRequest("/aether.v1.AuthService/Login")
		res, err := handler(context.Background(), req)
		require.NoError(t, err)
		assert.NotNil(t, res)

		var logMap map[string]any
		require.NoError(t, json.Unmarshal(buf.Bytes(), &logMap))
		assert.Equal(t, "custom rpc completed", logMap["msg"])
	})

	t.Run("error RPC call logs error, error_reason from connect.Error and grpc status, and root_cause", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

		rootErr := errors.New("inode entry missing")
		wrappedErr := fmt.Errorf("lookup failed: %w", rootErr)
		connectErr := connect.NewError(connect.CodeNotFound, wrappedErr)
		detail, detailErr := connect.NewErrorDetail(&errdetails.ErrorInfo{
			Reason: "ENOENT",
			Domain: "bowl.storage",
		})
		require.NoError(t, detailErr)
		connectErr.AddDetail(detail)

		interceptor := slogx.NewConnectInterceptor(nil, logger)
		handler := interceptor(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			return nil, connectErr
		})

		req := newTestRequest("/storage.v1.StorageService/Lookup")
		res, err := handler(context.Background(), req)
		require.ErrorIs(t, err, connectErr)
		assert.Nil(t, res)

		var logMap map[string]any
		require.NoError(t, json.Unmarshal(buf.Bytes(), &logMap))
		assert.Equal(t, "/storage.v1.StorageService/Lookup", logMap[slogx.DefaultProcedureKey])
		assert.Equal(t, "not_found", logMap[slogx.DefaultCodeKey])
		assert.Equal(t, "ENOENT", logMap[slogx.DefaultErrorReasonKey])
		assert.Contains(t, logMap[slogx.DefaultErrorKey], "lookup failed")
		assert.Equal(t, "inode entry missing", logMap[slogx.DefaultRootCauseKey])

		// 验证 gRPC *status.Status 包装错误同样提取 error_reason
		buf.Reset()
		st, stErr := status.New(codes.FailedPrecondition, "epoch mismatch").WithDetails(&errdetails.ErrorInfo{
			Reason: "EBUSY",
			Domain: "bowl.storage",
		})
		require.NoError(t, stErr)
		grpcHandler := interceptor(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			return nil, st.Err()
		})
		_, err = grpcHandler(context.Background(), req)
		require.Error(t, err)

		var grpcLogMap map[string]any
		require.NoError(t, json.Unmarshal(buf.Bytes(), &grpcLogMap))
		assert.Equal(t, "EBUSY", grpcLogMap[slogx.DefaultErrorReasonKey])
	})

	t.Run("WithLevelClassifier downgrades expected NotFound before Enabled gate with zero allocations and zero output", func(t *testing.T) {
		var buf bytes.Buffer
		// 生产默认 Info 级别：降级为 Debug 的请求不得输出，且不得触发属性提取或内存分配
		logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

		extractorCalled := false
		interceptor := slogx.NewConnectInterceptor(
			nil,
			logger,
			slogx.WithLevelClassifier(func(procedure string, code connect.Code, err error) slog.Level {
				if procedure == "/storage.v1.StorageService/Lookup" && code == connect.CodeNotFound {
					return slog.LevelDebug
				}
				return slogx.DefaultLevelClassifier(code, err)
			}),
			slogx.WithAttrExtractor(func(ctx context.Context, req connect.AnyRequest, res connect.AnyResponse, err error) []slog.Attr {
				extractorCalled = true
				return []slog.Attr{slog.String("disk_id", "disk-1")}
			}),
		)

		prebuiltErr := connect.NewError(connect.CodeNotFound, errors.New("expected enoent"))
		lookupReq := newTestRequest("/storage.v1.StorageService/Lookup")
		handler := interceptor(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			return nil, prebuiltErr
		})

		ctx := context.Background()
		allocs := testing.AllocsPerRun(100, func() {
			_, _ = handler(ctx, lookupReq)
		})
		assert.Equal(t, float64(0), allocs, "未达日志级别门控时必须实现 0 alloc")
		assert.Empty(t, buf.String(), "降级为 Debug 的预期 NotFound 在 Info 级别下必须零日志击穿")
		assert.False(t, extractorCalled, "未达门控级别时不得调用 WithAttrExtractor")

		// 反事实对照：非 Lookup 方法的 NotFound 保持 Warn 级别，正常输出并携带自定义业务属性
		otherReq := newTestRequest("/storage.v1.StorageService/DeleteDisk")
		_, err := handler(ctx, otherReq)
		require.ErrorIs(t, err, prebuiltErr)
		assert.True(t, extractorCalled)

		var logMap map[string]any
		require.NoError(t, json.Unmarshal(buf.Bytes(), &logMap))
		assert.Equal(t, "WARN", logMap["level"])
		assert.Equal(t, "disk-1", logMap["disk_id"])
	})

	t.Run("canceled error maps to canceled code and info level", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

		interceptor := slogx.NewConnectInterceptor(nil, logger)
		handler := interceptor(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			return nil, context.Canceled
		})

		req := newTestRequest("/aether.v1.AuthService/Stream")
		_, err := handler(context.Background(), req)
		require.ErrorIs(t, err, context.Canceled)

		var logMap map[string]any
		require.NoError(t, json.Unmarshal(buf.Bytes(), &logMap))
		assert.Equal(t, "canceled", logMap[slogx.DefaultCodeKey])
		assert.Equal(t, "INFO", logMap["level"])
	})

	t.Run("deadline exceeded error maps to deadline_exceeded code and warn level", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

		interceptor := slogx.NewConnectInterceptor(nil, logger)
		handler := interceptor(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			return nil, context.DeadlineExceeded
		})

		req := newTestRequest("/aether.v1.AuthService/TimeoutCall")
		_, err := handler(context.Background(), req)
		require.ErrorIs(t, err, context.DeadlineExceeded)

		var logMap map[string]any
		require.NoError(t, json.Unmarshal(buf.Bytes(), &logMap))
		assert.Equal(t, "deadline_exceeded", logMap[slogx.DefaultCodeKey])
		assert.Equal(t, "WARN", logMap["level"])
		assert.Contains(t, logMap[slogx.DefaultErrorKey], "context deadline exceeded")
	})

	t.Run("joined error extracts root cause", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

		err1 := errors.New("primary cause")
		err2 := errors.New("secondary cause")
		joinedErr := errors.Join(err1, err2)
		connectErr := connect.NewError(connect.CodeDataLoss, joinedErr)

		interceptor := slogx.NewConnectInterceptor(nil, logger)
		handler := interceptor(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			return nil, connectErr
		})

		_, err := handler(context.Background(), newTestRequest("/test"))
		require.ErrorIs(t, err, connectErr)

		var logMap map[string]any
		require.NoError(t, json.Unmarshal(buf.Bytes(), &logMap))
		assert.Equal(t, "data_loss", logMap[slogx.DefaultCodeKey])
		assert.Equal(t, "primary cause", logMap[slogx.DefaultRootCauseKey])
		assert.Equal(t, "ERROR", logMap["level"])
	})

	t.Run("nil logger defaults to slog.Default and records logs", func(t *testing.T) {
		orig := slog.Default()
		t.Cleanup(func() { slog.SetDefault(orig) })

		var buf bytes.Buffer
		testLogger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		slog.SetDefault(testLogger)

		interceptor := slogx.NewConnectInterceptor(nil, nil)

		handler := interceptor(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			return connect.NewResponse(&emptypb.Empty{}), nil
		})

		req := newTestRequest("/aether.v1.AuthService/Login")
		res, err := handler(context.Background(), req)
		require.NoError(t, err)
		assert.NotNil(t, res)

		var logMap map[string]any
		require.NoError(t, json.Unmarshal(buf.Bytes(), &logMap))
		assert.Equal(t, "/aether.v1.AuthService/Login", logMap[slogx.DefaultProcedureKey])
		assert.Equal(t, "ok", logMap[slogx.DefaultCodeKey])
		assert.Equal(t, slogx.DefaultConnectMessage, logMap["msg"])
	})
}

func TestNewMaskInterceptor(t *testing.T) {
	t.Run("nil error passes through", func(t *testing.T) {
		interceptor := slogx.NewMaskInterceptor(nil)
		unaryFunc := interceptor.WrapUnary(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			return connect.NewResponse(&emptypb.Empty{}), nil
		})

		res, err := unaryFunc(context.Background(), newTestRequest("/test"))
		require.NoError(t, err)
		assert.NotNil(t, res)
	})

	t.Run("non-internal/unknown errors pass through unmodified", func(t *testing.T) {
		interceptor := slogx.NewMaskInterceptor(nil)

		codes := []connect.Code{
			connect.CodeNotFound,
			connect.CodeInvalidArgument,
			connect.CodeUnauthenticated,
			connect.CodePermissionDenied,
			connect.CodeAlreadyExists,
		}

		for _, code := range codes {
			originalErr := connect.NewError(code, errors.New("sensitive business rule violation"))
			unaryFunc := interceptor.WrapUnary(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
				return nil, originalErr
			})

			_, err := unaryFunc(context.Background(), newTestRequest("/test"))
			require.Equal(t, originalErr, err)
			assert.Equal(t, code, connect.CodeOf(err))
		}
	})

	t.Run("CodeUnknown with context.Canceled or context.DeadlineExceeded passes through", func(t *testing.T) {
		interceptor := slogx.NewMaskInterceptor(nil)

		unaryCanceled := interceptor.WrapUnary(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			return nil, context.Canceled
		})
		_, err := unaryCanceled(context.Background(), newTestRequest("/test"))
		require.ErrorIs(t, err, context.Canceled)

		unaryTimeout := interceptor.WrapUnary(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			return nil, context.DeadlineExceeded
		})
		_, err = unaryTimeout(context.Background(), newTestRequest("/test"))
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("CodeCanceled and CodeDeadlineExceeded with sensitive details masks message and preserves Details and Meta", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		interceptor := slogx.NewMaskInterceptor(logger)

		origCanceledErr := connect.NewError(connect.CodeCanceled, errors.New("dial tcp 10.0.0.12:5432: secret: context canceled"))
		detail, err := connect.NewErrorDetail(&emptypb.Empty{})
		require.NoError(t, err)
		origCanceledErr.AddDetail(detail)
		origCanceledErr.Meta().Set("X-Trace-Id", "trace-cancel-123")

		unaryCanceled := interceptor.WrapUnary(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			return nil, origCanceledErr
		})
		_, maskedCanceled := unaryCanceled(context.Background(), newTestRequest("/test"))
		require.Error(t, maskedCanceled)
		assert.Equal(t, connect.CodeCanceled, connect.CodeOf(maskedCanceled))
		var connCancelErr *connect.Error
		require.True(t, errors.As(maskedCanceled, &connCancelErr))
		assert.Equal(t, "context canceled", connCancelErr.Message())
		assert.NotContains(t, maskedCanceled.Error(), "10.0.0.12")
		assert.NotContains(t, maskedCanceled.Error(), "secret")
		assert.Len(t, connCancelErr.Details(), 1)
		assert.Equal(t, "trace-cancel-123", connCancelErr.Meta().Get("X-Trace-Id"))

		origTimeoutErr := connect.NewError(connect.CodeDeadlineExceeded, errors.New("dial tcp 10.0.0.12:5432: secret: context deadline exceeded"))
		origTimeoutErr.AddDetail(detail)
		origTimeoutErr.Meta().Set("X-Trace-Id", "trace-timeout-123")

		unaryTimeout := interceptor.WrapUnary(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			return nil, origTimeoutErr
		})
		_, maskedTimeout := unaryTimeout(context.Background(), newTestRequest("/test"))
		require.Error(t, maskedTimeout)
		assert.Equal(t, connect.CodeDeadlineExceeded, connect.CodeOf(maskedTimeout))
		var connTimeoutErr *connect.Error
		require.True(t, errors.As(maskedTimeout, &connTimeoutErr))
		assert.Equal(t, "context deadline exceeded", connTimeoutErr.Message())
		assert.NotContains(t, maskedTimeout.Error(), "10.0.0.12")
		assert.NotContains(t, maskedTimeout.Error(), "secret")
		assert.Len(t, connTimeoutErr.Details(), 1)
		assert.Equal(t, "trace-timeout-123", connTimeoutErr.Meta().Get("X-Trace-Id"))
	})

	t.Run("CodeInternal masks message to internal server error and preserves Details and Meta", func(t *testing.T) {
		var buf bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

		interceptor := slogx.NewMaskInterceptor(logger)

		originalErr := connect.NewError(connect.CodeInternal, errors.New("SELECT * FROM sensitive_users WHERE password = 'secret' failed: conn closed"))
		detail, err := connect.NewErrorDetail(&emptypb.Empty{})
		require.NoError(t, err)
		originalErr.AddDetail(detail)
		originalErr.Meta().Set("X-Trace-Id", "trace-12345")
		originalErr.Meta().Add("X-Custom-Header", "val1")
		originalErr.Meta().Add("X-Custom-Header", "val2")

		unaryFunc := interceptor.WrapUnary(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			return nil, originalErr
		})

		_, maskedErr := unaryFunc(context.Background(), newTestRequest("/test"))
		require.Error(t, maskedErr)

		assert.Equal(t, connect.CodeInternal, connect.CodeOf(maskedErr))
		var connErr *connect.Error
		require.True(t, errors.As(maskedErr, &connErr))
		assert.Equal(t, "internal server error", connErr.Message())
		assert.NotContains(t, maskedErr.Error(), "sensitive_users")
		assert.NotContains(t, maskedErr.Error(), "secret")

		require.Len(t, connErr.Details(), 1)
		assert.Equal(t, "trace-12345", connErr.Meta().Get("X-Trace-Id"))
		assert.Equal(t, []string{"val1", "val2"}, connErr.Meta()["X-Custom-Header"])
		assert.Contains(t, buf.String(), "internal server error")
	})

	t.Run("CodeUnknown standard error masks to CodeInternal and internal server error", func(t *testing.T) {
		interceptor := slogx.NewMaskInterceptor(nil)

		rawErr := errors.New("unexpected panic in engine: pointer dereference")
		unaryFunc := interceptor.WrapUnary(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			return nil, rawErr
		})

		_, maskedErr := unaryFunc(context.Background(), newTestRequest("/test"))
		require.Error(t, maskedErr)
		assert.Equal(t, connect.CodeInternal, connect.CodeOf(maskedErr))

		var connErr *connect.Error
		require.True(t, errors.As(maskedErr, &connErr))
		assert.Equal(t, "internal server error", connErr.Message())
		assert.NotContains(t, maskedErr.Error(), "pointer dereference")
	})
}

func TestDefaultLevelClassifier(t *testing.T) {
	t.Run("nil error returns LevelInfo", func(t *testing.T) {
		level := slogx.DefaultLevelClassifier(connect.CodeInternal, nil)
		assert.Equal(t, slog.LevelInfo, level)
	})

	t.Run("server and unknown errors return LevelError", func(t *testing.T) {
		err := errors.New("boom")
		errorCodes := []connect.Code{
			connect.CodeInternal,
			connect.CodeUnknown,
			connect.CodeDataLoss,
			connect.CodeUnavailable,
		}
		for _, code := range errorCodes {
			assert.Equal(t, slog.LevelError, slogx.DefaultLevelClassifier(code, err))
		}
	})

	t.Run("canceled error returns LevelInfo", func(t *testing.T) {
		assert.Equal(t, slog.LevelInfo, slogx.DefaultLevelClassifier(connect.CodeCanceled, context.Canceled))
	})

	t.Run("unspecified or business codes fallback to LevelWarn", func(t *testing.T) {
		err := errors.New("business error")
		warnCodes := []connect.Code{
			connect.CodeInvalidArgument,
			connect.CodeNotFound,
			connect.CodeAlreadyExists,
			connect.CodePermissionDenied,
			connect.CodeResourceExhausted,
			connect.CodeFailedPrecondition,
			connect.CodeAborted,
			connect.CodeOutOfRange,
			connect.CodeUnimplemented,
			connect.CodeUnauthenticated,
			connect.CodeDeadlineExceeded,
			connect.Code(999),
		}
		for _, code := range warnCodes {
			assert.Equal(t, slog.LevelWarn, slogx.DefaultLevelClassifier(code, err))
		}
	})
}
