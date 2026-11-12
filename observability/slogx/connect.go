package slogx

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// DefaultConnectMessage 是处理完成的 RPC 请求的默认日志消息。
	DefaultConnectMessage = "handled rpc"
	// DefaultMessage 保持语义常量对齐。
	DefaultMessage = DefaultConnectMessage

	// DefaultProcedureKey 是 RPC 方法（procedure）的默认属性键。
	DefaultProcedureKey = "procedure"
	// DefaultDurationKey 是 RPC 耗时（duration）的默认属性键。
	DefaultDurationKey = "duration"
	// DefaultCodeKey 是 Connect 状态码（code）的默认属性键。
	DefaultCodeKey = "code"
	// DefaultErrorKey 是错误消息（error）的默认属性键。
	DefaultErrorKey = "error"
	// DefaultErrorReasonKey 是从 google.rpc.ErrorInfo 提取的结构化错误原因码（error_reason）属性键。
	DefaultErrorReasonKey = "error_reason"
	// DefaultRootCauseKey 是提取出的根因错误（root_cause）的默认属性键。
	DefaultRootCauseKey = "root_cause"

	// CodeOK 表示没有发生错误的成功 RPC 调用。
	CodeOK connect.Code = 0
)

// LevelClassifier 根据给定的 Connect 状态码与错误决定对应的 slog.Level。
type LevelClassifier func(code connect.Code, err error) slog.Level

// DefaultLevelClassifier 将 Connect 状态码映射到对应的 slog 日志级别：
// - Internal、Unknown、DataLoss、Unavailable -> LevelError
// - Canceled、成功调用 (err == nil) -> LevelInfo
// - InvalidArgument、Unauthenticated、NotFound 等 -> LevelWarn
func DefaultLevelClassifier(code connect.Code, err error) slog.Level {
	if err == nil {
		return slog.LevelInfo
	}
	switch code {
	case connect.CodeInternal,
		connect.CodeUnknown,
		connect.CodeDataLoss,
		connect.CodeUnavailable:
		return slog.LevelError
	case connect.CodeCanceled:
		return slog.LevelInfo
	default:
		return slog.LevelWarn
	}
}

type connectOptions struct {
	levelClassifier func(procedure string, code connect.Code, err error) slog.Level
	attrExtractor   func(ctx context.Context, req connect.AnyRequest, res connect.AnyResponse, err error) []slog.Attr
}

// ConnectOption 定义定制 ConnectRPC 访问日志拦截器的函数式选项。
type ConnectOption func(*connectOptions)

// WithLevelClassifier 注入按 RPC 方法名、Connect 状态码与错误动态决策日志级别的分类器。
// 分类判定发生在 logger.Enabled(ctx, level) 门控之前：当降级后的日志级别未开启时，跳过一切属性构造与内存分配。
func WithLevelClassifier(fn func(procedure string, code connect.Code, err error) slog.Level) ConnectOption {
	return func(o *connectOptions) {
		if fn != nil {
			o.levelClassifier = fn
		}
	}
}

// WithAttrExtractor 注入自定义业务属性提取器（仅在当前日志级别启用时调用）。
func WithAttrExtractor(fn func(ctx context.Context, req connect.AnyRequest, res connect.AnyResponse, err error) []slog.Attr) ConnectOption {
	return func(o *connectOptions) {
		if fn != nil {
			o.attrExtractor = fn
		}
	}
}

// NewConnectInterceptor 返回一个一元（unary）ConnectRPC 拦截器，使用结构化属性记录 RPC 调用日志。
// 显式依赖注入 cfg 与 logger，消除对全局 slog.Default() 的隐式依赖与并发单测竞态。若 logger 为 nil 则回退至 slog.Default()。
func NewConnectInterceptor(cfg Config, logger *slog.Logger, opts ...ConnectOption) connect.UnaryInterceptorFunc {
	message := DefaultConnectMessage
	if cfg != nil && cfg.Message() != "" {
		message = cfg.Message()
	}
	if logger == nil {
		logger = slog.Default()
	}

	options := connectOptions{
		levelClassifier: func(_ string, code connect.Code, err error) slog.Level {
			return DefaultLevelClassifier(code, err)
		},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}

	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			start := time.Now()
			res, err := next(ctx, req)
			duration := time.Since(start)

			var (
				code      connect.Code
				procedure string
			)
			if req != nil {
				procedure = req.Spec().Procedure
			}
			if err != nil {
				if connectErr, ok := err.(*connect.Error); ok {
					code = connectErr.Code()
				} else {
					code = connect.CodeOf(err)
					if code == connect.CodeUnknown {
						if errors.Is(err, context.Canceled) {
							code = connect.CodeCanceled
						} else if errors.Is(err, context.DeadlineExceeded) {
							code = connect.CodeDeadlineExceeded
						}
					}
				}
			} else {
				code = CodeOK
			}

			level := options.levelClassifier(procedure, code, err)

			if logger.Enabled(ctx, level) {
				attrs := make([]slog.Attr, 0, 8)
				attrs = append(attrs,
					slog.String(DefaultProcedureKey, procedure),
					slog.Duration(DefaultDurationKey, duration),
					slog.String(DefaultCodeKey, formatCode(code, err)),
				)

				if err != nil {
					attrs = append(attrs, slog.String(DefaultErrorKey, err.Error()))
					if reason := extractErrorReason(err); reason != "" {
						attrs = append(attrs, slog.String(DefaultErrorReasonKey, reason))
					}
					if rootCause := extractRootCause(err); rootCause != nil {
						attrs = append(attrs, slog.String(DefaultRootCauseKey, rootCause.Error()))
					}
				}

				if options.attrExtractor != nil {
					if extraAttrs := options.attrExtractor(ctx, req, res, err); len(extraAttrs) > 0 {
						attrs = append(attrs, extraAttrs...)
					}
				}

				logger.LogAttrs(ctx, level, message, attrs...)
			}

			// 日志拦截器保持“只读观测”，原样透传返回 err，避免掩蔽导致 OTel 链路追踪丢失错误根因。
			return res, err
		}
	}
}

// NewMaskInterceptor 返回一个一元（unary）ConnectRPC 拦截器，
// 用于在最外层对敏感内部错误（CodeInternal 与 CodeUnknown）以及 context 取消/超时错误进行脱敏保护，
// 防止底层 SQL 错误、数据库连接异常等原始内部细节泄露给公网调用方，
// 同时完整保留原始错误携带的 Details 与 Meta 元数据。
func NewMaskInterceptor(logger *slog.Logger) connect.Interceptor {
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			res, err := next(ctx, req)
			if err == nil {
				return res, nil
			}

			code := connect.CodeOf(err)
			if code == connect.CodeCanceled || (code == connect.CodeUnknown && errors.Is(err, context.Canceled)) {
				maskedErr := connect.NewError(connect.CodeCanceled, context.Canceled)
				copyConnectErrorMetadata(err, maskedErr)
				return nil, maskedErr
			}

			if code == connect.CodeDeadlineExceeded || (code == connect.CodeUnknown && errors.Is(err, context.DeadlineExceeded)) {
				maskedErr := connect.NewError(connect.CodeDeadlineExceeded, context.DeadlineExceeded)
				copyConnectErrorMetadata(err, maskedErr)
				return nil, maskedErr
			}

			if code == connect.CodeInternal || code == connect.CodeUnknown {
				if logger != nil {
					logger.ErrorContext(ctx, "internal server error masked",
						slog.String("code", code.String()),
						slog.String("error", err.Error()),
					)
				}
				maskedErr := connect.NewError(connect.CodeInternal, errors.New("internal server error"))
				copyConnectErrorMetadata(err, maskedErr)
				return nil, maskedErr
			}

			return res, err
		}
	})
}

func copyConnectErrorMetadata(src error, dst *connect.Error) {
	var connectErr *connect.Error
	if errors.As(src, &connectErr) {
		for _, detail := range connectErr.Details() {
			dst.AddDetail(detail)
		}
		meta := dst.Meta()
		for k, values := range connectErr.Meta() {
			meta[k] = append([]string(nil), values...)
		}
	}
}

func formatCode(code connect.Code, err error) string {
	if err == nil {
		return "ok"
	}
	return code.String()
}

// extractErrorReason 从 *connect.Error 或 gRPC *status.Status 中提取第一个非空的 google.rpc.ErrorInfo.Reason。
func extractErrorReason(err error) string {
	if err == nil {
		return ""
	}

	var connectErr *connect.Error
	if errors.As(err, &connectErr) {
		for _, detail := range connectErr.Details() {
			msg, valErr := detail.Value()
			if valErr != nil {
				continue
			}
			if info, ok := msg.(*errdetails.ErrorInfo); ok && info.GetReason() != "" {
				return info.GetReason()
			}
		}
	}

	for curr := err; curr != nil; curr = errors.Unwrap(curr) {
		if st, ok := status.FromError(curr); ok && st.Code() != codes.OK {
			if r := reasonFromGRPCStatus(st); r != "" {
				return r
			}
		}
	}

	var se interface{ GRPCStatus() *status.Status }
	if errors.As(err, &se) {
		if st := se.GRPCStatus(); st != nil && st.Code() != codes.OK {
			if r := reasonFromGRPCStatus(st); r != "" {
				return r
			}
		}
	}

	return ""
}

func reasonFromGRPCStatus(st *status.Status) string {
	for _, detail := range st.Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok && info.GetReason() != "" {
			return info.GetReason()
		}
	}
	if proto := st.Proto(); proto != nil {
		for _, anyDetail := range proto.GetDetails() {
			if anyDetail == nil {
				continue
			}
			msg, err := anyDetail.UnmarshalNew()
			if err != nil {
				continue
			}
			if info, ok := msg.(*errdetails.ErrorInfo); ok && info.GetReason() != "" {
				return info.GetReason()
			}
		}
	}
	return ""
}

// extractRootCause 从错误链中提取最内层的根因错误。
// 基于 Go 标准库错误链规范（errors.Unwrap 与 errors.Join 切片解包契约），移除人为递归深度限制。
func extractRootCause(err error) error {
	var connectErr *connect.Error
	if errors.As(err, &connectErr) {
		err = connectErr.Unwrap()
	}
	for err != nil {
		if next := errors.Unwrap(err); next != nil {
			err = next
			continue
		}
		if u, ok := err.(interface{ Unwrap() []error }); ok {
			var found error
			for _, e := range u.Unwrap() {
				if e != nil {
					found = e
					break
				}
			}
			if found != nil {
				err = found
				continue
			}
		}
		break
	}
	return err
}
