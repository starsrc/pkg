package serverx_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/starsrc/pkg/observability/slogx"
	"github.com/starsrc/pkg/serverx"
)

type dummyObservabilityConfig struct {
	level            slog.Level
	format           string
	message          string
	recordTraceFlags bool
	sampledOnly      bool
	trustRemote      bool
}

func (d dummyObservabilityConfig) SlogLevel() slog.Level  { return d.level }
func (d dummyObservabilityConfig) Format() string         { return d.format }
func (d dummyObservabilityConfig) Message() string        { return d.message }
func (d dummyObservabilityConfig) RecordTraceFlags() bool { return d.recordTraceFlags }
func (d dummyObservabilityConfig) SampledOnly() bool      { return d.sampledOnly }
func (d dummyObservabilityConfig) TrustRemote() bool      { return d.trustRemote }

type orderInterceptor struct {
	name   string
	events *[]string
}

func (o *orderInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		*o.events = append(*o.events, "enter:"+o.name)
		res, err := next(ctx, req)
		*o.events = append(*o.events, "leave:"+o.name)
		return res, err
	}
}

func (o *orderInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (o *orderInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

func TestNewInterceptors_StructureAndNilFiltering(t *testing.T) {
	logger := slog.Default()
	cfg := dummyObservabilityConfig{trustRemote: true}

	t.Run("基础拦截器链结构（无extra）包含5个拦截器", func(t *testing.T) {
		interceptors, err := serverx.NewInterceptors(logger, cfg)
		require.NoError(t, err)
		assert.Len(t, interceptors, 5) // mask, otel, errorReason, slogx, validate
	})

	t.Run("追加extra并自动过滤nil元素", func(t *testing.T) {
		events := []string{}
		extra1 := &orderInterceptor{name: "extra1", events: &events}
		extra2 := &orderInterceptor{name: "extra2", events: &events}

		interceptors, err := serverx.NewInterceptors(logger, cfg, nil, extra1, nil, extra2, nil)
		require.NoError(t, err)
		assert.Len(t, interceptors, 7) // mask, otel, errorReason, slogx, extra1, extra2, validate
	})

	t.Run("cfg为nil时安全降级不panic", func(t *testing.T) {
		interceptors, err := serverx.NewInterceptors(nil, nil)
		require.NoError(t, err)
		assert.Len(t, interceptors, 5)
	})

	t.Run("NewHandlerOptions包装可用性", func(t *testing.T) {
		opts, err := serverx.NewHandlerOptions(logger, cfg)
		require.NoError(t, err)
		assert.Len(t, opts, 1)
	})

	t.Run("TrustRemote为false时依然正常构建", func(t *testing.T) {
		cfgNoTrust := dummyObservabilityConfig{trustRemote: false}
		interceptors, err := serverx.NewInterceptors(logger, cfgNoTrust)
		require.NoError(t, err)
		assert.Len(t, interceptors, 5)
	})
}

func TestNewInterceptors_OnionOrderingAndMaskBehavior(t *testing.T) {
	origTP := otel.GetTracerProvider()
	origProp := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(origTP)
		otel.SetTextMapPropagator(origProp)
	})

	spanRecorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(spanRecorder),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	_ = otelconnect.WithTracerProvider(tp)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	var logBuf bytes.Buffer
	cfg := dummyObservabilityConfig{
		level:       slog.LevelInfo,
		message:     "access log",
		trustRemote: false,
	}
	baseHandler := slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(slogx.NewOtelHandler(baseHandler, cfg))

	events := []string{}
	extra := &orderInterceptor{name: "business_auth", events: &events}

	interceptors, err := serverx.NewInterceptors(logger, cfg, extra)
	require.NoError(t, err)

	// 组装洋葱调用链：从后向前包装 next
	// 洋葱顺序：interceptors[0](interceptors[1](...handler...))
	// 即 mask(otel(slogx(extra(validator(handler))))))
	rawSensitiveErr := errors.New("db connection reset by peer")
	handler := func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		events = append(events, "handler_executed")
		return nil, rawSensitiveErr
	}

	chained := handler
	for i := len(interceptors) - 1; i >= 0; i-- {
		chained = interceptors[i].WrapUnary(chained)
	}

	ctx := context.Background()
	req := connect.NewRequest(&emptypb.Empty{})

	// 执行调用
	resp, callErr := chained(ctx, req)
	assert.Nil(t, resp)
	require.Error(t, callErr)

	// 1. 最外层 mask 拦截器必须将未包装的敏感错误脱敏为统一的 internal server error
	assert.Equal(t, connect.CodeInternal, connect.CodeOf(callErr))
	assert.NotContains(t, callErr.Error(), "db connection reset by peer")
	assert.Contains(t, callErr.Error(), "internal server error")

	// 2. 中间层 access log (slogx) 位于 mask 内部，应当能观测到原始错误，且必须先于外层 mask 的日志输出
	lines := strings.Split(strings.TrimSpace(logBuf.String()), "\n")
	accessLogIdx := -1
	maskedLogIdx := -1
	var accessLogEntry map[string]any

	for idx, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		var record map[string]any
		err := json.Unmarshal([]byte(trimmed), &record)
		require.NoError(t, err, "解析 JSON 日志失败: %s", trimmed)

		msg, _ := record["msg"].(string)
		if msg == "access log" {
			accessLogIdx = idx
			accessLogEntry = record
		} else if msg == "internal server error masked" {
			maskedLogIdx = idx
		}
	}

	require.NotEqual(t, -1, accessLogIdx, "日志中应包含 access log 记录")
	require.NotEqual(t, -1, maskedLogIdx, "日志中应包含 internal server error masked 记录")

	// 断言 access log 的 error 与 root_cause 字段包含/等于原始敏感错误
	assert.Equal(t, "db connection reset by peer", accessLogEntry["error"])
	assert.Equal(t, "db connection reset by peer", accessLogEntry["root_cause"])

	// 验证 otel 处于 slogx 外层：slogx 访问日志中包含非空的 trace_id 与 span_id
	traceID, hasTraceID := accessLogEntry["trace_id"].(string)
	spanID, hasSpanID := accessLogEntry["span_id"].(string)
	assert.True(t, hasTraceID && traceID != "", "access log 应包含非空的 trace_id（证明 otel 位于 slogx 外层）")
	assert.True(t, hasSpanID && spanID != "", "access log 应包含非空的 span_id（证明 otel 位于 slogx 外层）")

	// 验证 otel 处于 mask 内层：spanRecorder.Ended() 中捕获的服务端 Span 状态描述或错误事件中保留了未脱敏的原始内部错误
	endedSpans := spanRecorder.Ended()
	require.NotEmpty(t, endedSpans, "服务端 Span 应当已被记录并结束")
	serverSpan := endedSpans[0]
	assert.Equal(t, traceID, serverSpan.SpanContext().TraceID().String())
	assert.Equal(t, spanID, serverSpan.SpanContext().SpanID().String())

	var foundRawErrInSpan bool
	if strings.Contains(serverSpan.Status().Description, "db connection reset by peer") {
		foundRawErrInSpan = true
	}
	for _, event := range serverSpan.Events() {
		for _, attr := range event.Attributes {
			if strings.Contains(attr.Value.AsString(), "db connection reset by peer") {
				foundRawErrInSpan = true
				break
			}
		}
	}
	assert.True(t, foundRawErrInSpan, "spanRecorder.Ended() 中捕获的服务端 Span 状态描述或错误事件中应保留未脱敏的原始内部错误 'db connection reset by peer'（证明 otel 位于 mask 内层）")

	// 洋葱顺序检验：slogx 位于 mask 内层，内层 slogx 先写完 access log，外层 mask 随后捕获并输出
	assert.Less(t, accessLogIdx, maskedLogIdx, "access log 应先于 internal server error masked 记录输出")

	// 3. 业务拦截器在 handler 之前执行
	require.Equal(t, []string{"enter:business_auth", "handler_executed", "leave:business_auth"}, events)
}

func TestNewInterceptorsWithValidator(t *testing.T) {
	logger := slog.Default()
	cfg := dummyObservabilityConfig{trustRemote: true}

	t.Run("显式注入自定义验证拦截器位于最内层", func(t *testing.T) {
		events := []string{}
		customValidator := &orderInterceptor{name: "custom_validator", events: &events}
		bizInterceptor := &orderInterceptor{name: "biz_auth", events: &events}

		interceptors, err := serverx.NewInterceptorsWithValidator(logger, cfg, customValidator, bizInterceptor)
		require.NoError(t, err)
		require.Len(t, interceptors, 6) // mask, otel, errorReason, slogx, biz_auth, custom_validator

		handler := func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			events = append(events, "handler")
			return connect.NewResponse(&emptypb.Empty{}), nil
		}

		chained := handler
		for i := len(interceptors) - 1; i >= 0; i-- {
			chained = interceptors[i].WrapUnary(chained)
		}

		_, err = chained(context.Background(), connect.NewRequest(&emptypb.Empty{}))
		require.NoError(t, err)

		// 验证执行时序：biz_auth 先于 custom_validator，custom_validator 紧贴 handler
		assert.Equal(t, []string{
			"enter:biz_auth",
			"enter:custom_validator",
			"handler",
			"leave:custom_validator",
			"leave:biz_auth",
		}, events)
	})

	t.Run("validator为nil时安全降级为默认校验拦截器", func(t *testing.T) {
		interceptors, err := serverx.NewInterceptorsWithValidator(logger, cfg, nil)
		require.NoError(t, err)
		assert.Len(t, interceptors, 5) // mask, otel, errorReason, slogx, default validate
	})

	t.Run("通过WithValidatorInterceptor辅助函数注入并在NewInterceptors中生效", func(t *testing.T) {
		events := []string{}
		customValidator := &orderInterceptor{name: "custom_validator", events: &events}
		bizInterceptor := &orderInterceptor{name: "biz_auth", events: &events}

		// 将 WithValidatorInterceptor 作为 extra 传入 NewInterceptors
		interceptors, err := serverx.NewInterceptors(
			logger,
			cfg,
			bizInterceptor,
			serverx.WithValidatorInterceptor(customValidator),
		)
		require.NoError(t, err)
		require.Len(t, interceptors, 6) // mask, otel, errorReason, slogx, biz_auth, custom_validator

		handler := func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			events = append(events, "handler")
			return connect.NewResponse(&emptypb.Empty{}), nil
		}

		chained := handler
		for i := len(interceptors) - 1; i >= 0; i-- {
			chained = interceptors[i].WrapUnary(chained)
		}

		_, err = chained(context.Background(), connect.NewRequest(&emptypb.Empty{}))
		require.NoError(t, err)

		assert.Equal(t, []string{
			"enter:biz_auth",
			"enter:custom_validator",
			"handler",
			"leave:custom_validator",
			"leave:biz_auth",
		}, events)
	})

	t.Run("WithValidatorInterceptor传入nil时返回nil且被自动过滤", func(t *testing.T) {
		nilWrapper := serverx.WithValidatorInterceptor(nil)
		assert.Nil(t, nilWrapper)

		interceptors, err := serverx.NewInterceptors(logger, cfg, nilWrapper)
		require.NoError(t, err)
		assert.Len(t, interceptors, 5) // mask, otel, errorReason, slogx, default validate
	})

	t.Run("NewHandlerOptionsWithValidator包装可用性", func(t *testing.T) {
		events := []string{}
		customValidator := &orderInterceptor{name: "custom_validator", events: &events}
		extraInterceptor := &orderInterceptor{name: "extra_auth", events: &events}

		opts, err := serverx.NewHandlerOptionsWithValidator(logger, cfg, customValidator, extraInterceptor)
		require.NoError(t, err)
		require.NotEmpty(t, opts)

		const testProcedure = "/test.v1.TestService/Ping"
		mux := http.NewServeMux()
		mux.Handle(testProcedure, connect.NewUnaryHandler(
			testProcedure,
			func(ctx context.Context, req *connect.Request[emptypb.Empty]) (*connect.Response[emptypb.Empty], error) {
				events = append(events, "handler")
				return connect.NewResponse(&emptypb.Empty{}), nil
			},
			opts...,
		))

		srv := httptest.NewServer(mux)
		defer srv.Close()

		client := connect.NewClient[emptypb.Empty, emptypb.Empty](
			srv.Client(),
			srv.URL+testProcedure,
		)

		_, err = client.CallUnary(context.Background(), connect.NewRequest(&emptypb.Empty{}))
		require.NoError(t, err)

		assert.Equal(t, []string{
			"enter:extra_auth",
			"enter:custom_validator",
			"handler",
			"leave:custom_validator",
			"leave:extra_auth",
		}, events)
	})
}

func TestNewInterceptors_TrustRemoteBehavior(t *testing.T) {
	origTP := otel.GetTracerProvider()
	origProp := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(origTP)
		otel.SetTextMapPropagator(origProp)
	})

	const (
		remoteTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
		remoteSpanID  = "00f067aa0ba902b7"
	)
	traceparent := fmt.Sprintf("00-%s-%s-01", remoteTraceID, remoteSpanID)

	runInvocation := func(t *testing.T, trustRemote bool) (trace.SpanContext, tracetest.SpanStub) {
		t.Helper()

		sr := tracetest.NewSpanRecorder()
		tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
		t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

		otel.SetTracerProvider(tp)
		otel.SetTextMapPropagator(propagation.TraceContext{})

		cfg := dummyObservabilityConfig{
			level:       slog.LevelInfo,
			trustRemote: trustRemote,
		}

		interceptors, err := serverx.NewInterceptors(slog.Default(), cfg)
		require.NoError(t, err)

		var capturedSpanCtx trace.SpanContext
		handler := func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			capturedSpanCtx = trace.SpanFromContext(ctx).SpanContext()
			return connect.NewResponse(&emptypb.Empty{}), nil
		}

		chained := handler
		for i := len(interceptors) - 1; i >= 0; i-- {
			chained = interceptors[i].WrapUnary(chained)
		}

		req := connect.NewRequest(&emptypb.Empty{})
		req.Header().Set("traceparent", traceparent)

		_, err = chained(context.Background(), req)
		require.NoError(t, err)

		endedSpans := sr.Ended()
		require.NotEmpty(t, endedSpans, "服务端 Span 应当已被记录并结束")
		return capturedSpanCtx, tracetest.SpanStubFromReadOnlySpan(endedSpans[0])
	}

	t.Run("TrustRemote为true时服务端Span继承远端TraceID与ParentSpanID", func(t *testing.T) {
		spanCtx, recordedSpan := runInvocation(t, true)
		assert.True(t, spanCtx.IsValid())
		assert.Equal(t, remoteTraceID, spanCtx.TraceID().String(), "应继承远端 TraceID")
		assert.Equal(t, remoteTraceID, recordedSpan.SpanContext.TraceID().String())
		assert.Equal(t, remoteSpanID, recordedSpan.Parent.SpanID().String(), "ParentSpanID 应匹配远端 SpanID")
	})

	t.Run("TrustRemote为false时服务端Span生成全新的根TraceID且不继承远端父Span", func(t *testing.T) {
		spanCtx, recordedSpan := runInvocation(t, false)
		assert.True(t, spanCtx.IsValid())
		assert.NotEqual(t, remoteTraceID, spanCtx.TraceID().String(), "不得继承远端 TraceID，应生成全新 TraceID")
		assert.NotEqual(t, remoteTraceID, recordedSpan.SpanContext.TraceID().String())
		assert.False(t, recordedSpan.Parent.IsValid(), "不信任远端追踪上下文时不应将远端 Span 作为父 Span")
	})
}

func TestNewInterceptors_Metrics_ErrorReason_ResponseHeader_And_ConnectLogOptions(t *testing.T) {
	origTP := otel.GetTracerProvider()
	origMP := otel.GetMeterProvider()
	origProp := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(origTP)
		otel.SetMeterProvider(origMP)
		otel.SetTextMapPropagator(origProp)
	})

	spanRecorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(spanRecorder),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	metricReader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(metricReader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	var logBuf bytes.Buffer
	cfg := dummyObservabilityConfig{
		level:       slog.LevelInfo,
		trustRemote: true,
	}
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	const okProcedure = "/storage.v1.StorageService/StatFs"
	const lookupProcedure = "/storage.v1.StorageService/Lookup"
	const writeProcedure = "/storage.v1.StorageService/Write"

	opts, err := serverx.NewHandlerOptions(
		logger,
		cfg,
		serverx.WithConnectLogOptions(
			slogx.WithLevelClassifier(func(procedure string, code connect.Code, fnErr error) slog.Level {
				if procedure == lookupProcedure && code == connect.CodeNotFound {
					return slog.LevelDebug
				}
				return slogx.DefaultLevelClassifier(code, fnErr)
			}),
			slogx.WithAttrExtractor(func(context.Context, connect.AnyRequest, connect.AnyResponse, error) []slog.Attr {
				return []slog.Attr{slog.String("disk_id", "disk-42")}
			}),
		),
	)
	require.NoError(t, err)

	makeConnectErrWithReason := func(code connect.Code, reason string) error {
		cErr := connect.NewError(code, errors.New(reason))
		detail, dErr := connect.NewErrorDetail(&errdetails.ErrorInfo{
			Reason: reason,
			Domain: "bowl.storage",
		})
		require.NoError(t, dErr)
		cErr.AddDetail(detail)
		return cErr
	}

	mux := http.NewServeMux()
	mux.Handle(okProcedure, connect.NewUnaryHandler(
		okProcedure,
		func(context.Context, *connect.Request[emptypb.Empty]) (*connect.Response[emptypb.Empty], error) {
			return connect.NewResponse(&emptypb.Empty{}), nil
		},
		opts...,
	))
	mux.Handle(lookupProcedure, connect.NewUnaryHandler(
		lookupProcedure,
		func(context.Context, *connect.Request[emptypb.Empty]) (*connect.Response[emptypb.Empty], error) {
			return nil, makeConnectErrWithReason(connect.CodeNotFound, "ENOENT")
		},
		opts...,
	))
	mux.Handle(writeProcedure, connect.NewUnaryHandler(
		writeProcedure,
		func(context.Context, *connect.Request[emptypb.Empty]) (*connect.Response[emptypb.Empty], error) {
			return nil, makeConnectErrWithReason(connect.CodeFailedPrecondition, "EBUSY")
		},
		opts...,
	))

	srv := httptest.NewServer(mux)
	defer srv.Close()

	// 0. 调用成功 RPC：验证 WithPropagateResponseHeader 在正常响应头中注入 traceparent
	okClient := connect.NewClient[emptypb.Empty, emptypb.Empty](srv.Client(), srv.URL+okProcedure)
	okRes, okErr := okClient.CallUnary(context.Background(), connect.NewRequest(&emptypb.Empty{}))
	require.NoError(t, okErr)
	assert.NotEmpty(t, okRes.Header().Get("traceparent"), "成功响应头应携带 traceparent")
	logBuf.Reset()

	// 1. 调用 Lookup：返回 ENOENT -> 日志被 WithLevelClassifier 降级抑制（0 输出），但 Span 与 RPC 指标仍记录 rpc.error_reason="ENOENT"
	lookupClient := connect.NewClient[emptypb.Empty, emptypb.Empty](srv.Client(), srv.URL+lookupProcedure)
	_, lookupErr := lookupClient.CallUnary(context.Background(), connect.NewRequest(&emptypb.Empty{}))
	require.Error(t, lookupErr)
	assert.Empty(t, logBuf.String(), "Lookup 的预期 ENOENT 应通过 WithConnectLogOptions 在门控前静默")

	// 验证错误响应同样向客户端回传了服务端 traceparent 响应头
	var cErr *connect.Error
	require.ErrorAs(t, lookupErr, &cErr)
	assert.NotEmpty(t, cErr.Meta().Get("traceparent"), "错误响应头亦应携带 traceparent")

	// 2. 调用 Write：返回 EBUSY -> 输出唯一 1 条结构化日志，携带 error_reason="EBUSY" 与自定义 disk_id="disk-42"
	writeClient := connect.NewClient[emptypb.Empty, emptypb.Empty](srv.Client(), srv.URL+writeProcedure)
	_, writeErr := writeClient.CallUnary(context.Background(), connect.NewRequest(&emptypb.Empty{}))
	require.Error(t, writeErr)

	lines := strings.Split(strings.TrimSpace(logBuf.String()), "\n")
	require.Len(t, lines, 1, "每次 RPC 只能有唯一一条访问日志（根除双倍日志）")
	var logRecord map[string]any
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &logRecord))
	assert.Equal(t, "EBUSY", logRecord[slogx.DefaultErrorReasonKey])
	assert.Equal(t, "disk-42", logRecord["disk_id"])

	// 3. 验证 Span 属性包含 rpc.error_reason
	ended := spanRecorder.Ended()
	require.Len(t, ended, 3)
	spanReasons := make([]string, 0, 2)
	for _, sp := range ended {
		for _, attr := range sp.Attributes() {
			if string(attr.Key) == "rpc.error_reason" {
				spanReasons = append(spanReasons, attr.Value.AsString())
			}
		}
	}
	assert.ElementsMatch(t, []string{"ENOENT", "EBUSY"}, spanReasons)

	// 4. 验证 OTel RPC 指标（移除 WithoutMetrics 后默认开启）携带 Labeler 注入的 rpc.error_reason 标签
	var rm metricdata.ResourceMetrics
	require.NoError(t, metricReader.Collect(context.Background(), &rm))

	var metricReasons []string
	var foundDurationMetric bool
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if strings.Contains(m.Name, "duration") {
				foundDurationMetric = true
				hist, ok := m.Data.(metricdata.Histogram[int64])
				if ok {
					for _, dp := range hist.DataPoints {
						if val, has := dp.Attributes.Value(attribute.Key("rpc.error_reason")); has {
							metricReasons = append(metricReasons, val.AsString())
						}
					}
				}
				histFloat, okFloat := m.Data.(metricdata.Histogram[float64])
				if okFloat {
					for _, dp := range histFloat.DataPoints {
						if val, has := dp.Attributes.Value(attribute.Key("rpc.error_reason")); has {
							metricReasons = append(metricReasons, val.AsString())
						}
					}
				}
			}
		}
	}
	assert.True(t, foundDurationMetric, "应当采集并导出 ConnectRPC 服务端耗时分布指标")
	assert.ElementsMatch(t, []string{"ENOENT", "EBUSY"}, metricReasons, "RPC 指标必须包含 Labeler 注入的 rpc.error_reason 维度")
}
