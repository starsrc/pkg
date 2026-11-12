package serverx

import (
	"context"
	"fmt"
	"log/slog"

	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
	"connectrpc.com/validate"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/starsrc/pkg/observability/slogx"
)

const attrKeyRPCErrorReason = "rpc.error_reason"

// ObservabilityConfig 定义拦截器流水线所需的日志与链路追踪配置契约。
type ObservabilityConfig interface {
	slogx.Config
	// TrustRemote 返回是否信任上游传播的链路追踪上下文。
	TrustRemote() bool
}

// validatorInterceptorWrapper 包装自定义参数校验拦截器，用于在拦截器列表中标识其应置于最内层。
type validatorInterceptorWrapper struct {
	connect.Interceptor
}

// WithValidatorInterceptor 将指定的 interceptor 标记为最内层校验拦截器。
// 若 interceptor 为 nil 则返回 nil。
func WithValidatorInterceptor(interceptor connect.Interceptor) connect.Interceptor {
	if interceptor == nil {
		return nil
	}
	return validatorInterceptorWrapper{Interceptor: interceptor}
}

// connectLogOptionsWrapper 承载透传至内部唯一 slogx.NewConnectInterceptor 的配置选项。
type connectLogOptionsWrapper struct {
	connect.Interceptor
	opts []slogx.ConnectOption
}

// WithConnectLogOptions 将一组 slogx.ConnectOption 包装为拦截器标记项，
// 透传给 serverx 内部唯一构建的访问日志拦截器（如按 RPC+Code 动态定级的 WithLevelClassifier 或业务属性提取器）。
func WithConnectLogOptions(opts ...slogx.ConnectOption) connect.Interceptor {
	var valid []slogx.ConnectOption
	for _, opt := range opts {
		if opt != nil {
			valid = append(valid, opt)
		}
	}
	if len(valid) == 0 {
		return nil
	}
	return connectLogOptionsWrapper{opts: valid}
}

type errorReasonInterceptor struct{}

// NewErrorReasonInterceptor 返回轻量错误原因标注拦截器（需紧贴置于 otelconnect 拦截器内层）：
// 当下游返回携带 google.rpc.ErrorInfo.Reason 的错误时，自动将有限枚举原因码（如 ENOENT、EBUSY）
// 同步写入活跃 Span 属性 rpc.error_reason 与 otelconnect.Labeler（驱动 RPC 指标维度聚合）。
func NewErrorReasonInterceptor() connect.Interceptor {
	return errorReasonInterceptor{}
}

func recordErrorReason(ctx context.Context, err error) {
	if err == nil {
		return
	}
	if cErr, ok := err.(*connect.Error); ok && cErr.Meta().Get("traceparent") == "" {
		otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(cErr.Meta()))
	}
	info := ExtractErrorInfo(err)
	if info == nil || info.GetReason() == "" {
		return
	}
	attr := attribute.String(attrKeyRPCErrorReason, info.GetReason())
	if span := trace.SpanFromContext(ctx); span.IsRecording() {
		span.SetAttributes(attr)
	}
	if labeler, ok := otelconnect.LabelerFromContext(ctx); ok {
		labeler.Add(attr)
	}
}

func (errorReasonInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		res, err := next(ctx, req)
		if err != nil {
			recordErrorReason(ctx, err)
		}
		return res, err
	}
}

func (errorReasonInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		return next(ctx, spec)
	}
}

func (errorReasonInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		err := next(ctx, conn)
		if err != nil {
			recordErrorReason(ctx, err)
		}
		return err
	}
}

// NewInterceptorsWithValidator 严格按洋葱模型顺序组装标准 ConnectRPC 拦截器流水线：
// 1. slogx.NewMaskInterceptor(logger) 最外层：兜底异常脱敏，确保下游产生的所有错误均先被脱敏
// 2. otelconnect.NewInterceptor(...) 次外层：分布式追踪上下文传播、响应头回传与基于全局 MeterProvider 的 RPC 指标采集
// 3. NewErrorReasonInterceptor()：提取 ErrorInfo.Reason 写入 Span 与 OTel RPC 指标 Labeler
// 4. slogx.NewConnectInterceptor(cfg, logger, logOpts...)：结构化访问日志，记录脱敏前的原始错误并携带 OTel TraceID
// 5. extra... 可选业务拦截器插槽（自动过滤 nil 元素、日志配置标记与自定义校验标记）
// 6. validator 最内层：Protobuf 契约参数校验（若为 nil 则降级使用 validate.NewInterceptor()）
func NewInterceptorsWithValidator(
	logger *slog.Logger,
	cfg ObservabilityConfig,
	validator connect.Interceptor,
	extra ...connect.Interceptor,
) ([]connect.Interceptor, error) {
	otelOpts := []otelconnect.Option{
		otelconnect.WithPropagateResponseHeader(),
	}
	if cfg != nil && cfg.TrustRemote() {
		otelOpts = append(otelOpts, otelconnect.WithTrustRemote())
	}

	otelInterceptor, err := otelconnect.NewInterceptor(otelOpts...)
	if err != nil {
		return nil, fmt.Errorf("create otelconnect interceptor: %w", err)
	}

	var (
		extractedValidator connect.Interceptor
		logOpts            []slogx.ConnectOption
	)
	filteredExtra := make([]connect.Interceptor, 0, len(extra))
	for _, item := range extra {
		if item == nil {
			continue
		}
		if wrapper, ok := item.(validatorInterceptorWrapper); ok {
			if extractedValidator == nil {
				extractedValidator = wrapper.Interceptor
			}
			continue
		}
		if logWrapper, ok := item.(connectLogOptionsWrapper); ok {
			logOpts = append(logOpts, logWrapper.opts...)
			continue
		}
		filteredExtra = append(filteredExtra, item)
	}

	maskInterceptor := slogx.NewMaskInterceptor(logger)
	reasonInterceptor := NewErrorReasonInterceptor()
	connectLogInterceptor := slogx.NewConnectInterceptor(cfg, logger, logOpts...)

	targetValidator := validator
	if targetValidator != nil {
		if wrapper, ok := targetValidator.(validatorInterceptorWrapper); ok {
			targetValidator = wrapper.Interceptor
		}
	} else if extractedValidator != nil {
		targetValidator = extractedValidator
	} else {
		targetValidator = validate.NewInterceptor()
	}

	interceptors := make([]connect.Interceptor, 0, 5+len(filteredExtra))
	interceptors = append(
		interceptors,
		maskInterceptor,
		otelInterceptor,
		reasonInterceptor,
		connectLogInterceptor,
	)
	interceptors = append(interceptors, filteredExtra...)
	interceptors = append(interceptors, targetValidator)

	return interceptors, nil
}

// NewInterceptors 严格按洋葱模型顺序组装标准 ConnectRPC 拦截器流水线。
// 默认以 validate.NewInterceptor() 作为最内层校验拦截器；支持通过 WithConnectLogOptions 与 WithValidatorInterceptor 定制。
func NewInterceptors(
	logger *slog.Logger,
	cfg ObservabilityConfig,
	extra ...connect.Interceptor,
) ([]connect.Interceptor, error) {
	return NewInterceptorsWithValidator(logger, cfg, nil, extra...)
}

// NewHandlerOptionsWithValidator 便捷包装 NewInterceptorsWithValidator，
// 直接返回可注入 ConnectRPC Handler 的 []connect.HandlerOption。
func NewHandlerOptionsWithValidator(
	logger *slog.Logger,
	cfg ObservabilityConfig,
	validator connect.Interceptor,
	extra ...connect.Interceptor,
) ([]connect.HandlerOption, error) {
	interceptors, err := NewInterceptorsWithValidator(logger, cfg, validator, extra...)
	if err != nil {
		return nil, err
	}
	return []connect.HandlerOption{connect.WithInterceptors(interceptors...)}, nil
}

// NewHandlerOptions 便捷包装 NewInterceptors，直接返回可注入 ConnectRPC Handler 的 []connect.HandlerOption。
func NewHandlerOptions(
	logger *slog.Logger,
	cfg ObservabilityConfig,
	extra ...connect.Interceptor,
) ([]connect.HandlerOption, error) {
	return NewHandlerOptionsWithValidator(logger, cfg, nil, extra...)
}
