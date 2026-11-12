package serverx

import (
	"errors"

	"connectrpc.com/connect"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// grpcCodeToConnectCode 映射 gRPC 状态码至 ConnectRPC 错误码。
func grpcCodeToConnectCode(c codes.Code) connect.Code {
	switch c {
	case codes.Canceled:
		return connect.CodeCanceled
	case codes.Unknown:
		return connect.CodeUnknown
	case codes.InvalidArgument:
		return connect.CodeInvalidArgument
	case codes.DeadlineExceeded:
		return connect.CodeDeadlineExceeded
	case codes.NotFound:
		return connect.CodeNotFound
	case codes.AlreadyExists:
		return connect.CodeAlreadyExists
	case codes.PermissionDenied:
		return connect.CodePermissionDenied
	case codes.ResourceExhausted:
		return connect.CodeResourceExhausted
	case codes.FailedPrecondition:
		return connect.CodeFailedPrecondition
	case codes.Aborted:
		return connect.CodeAborted
	case codes.OutOfRange:
		return connect.CodeOutOfRange
	case codes.Unimplemented:
		return connect.CodeUnimplemented
	case codes.Internal:
		return connect.CodeInternal
	case codes.Unavailable:
		return connect.CodeUnavailable
	case codes.DataLoss:
		return connect.CodeDataLoss
	case codes.Unauthenticated:
		return connect.CodeUnauthenticated
	default:
		return connect.CodeUnknown
	}
}

// StatusToConnectError 将 gRPC *status.Status 转换为 *connect.Error。
// 当 st == nil 或 st.Code() == codes.OK 时返回 nil。
// 转换时将 17 种 gRPC 状态码 1:1 映射为对应的 connect.Code，
// 并将 status 携带的所有 Protobuf Details 完整附加到生成的 *connect.Error 上。
func StatusToConnectError(st *status.Status) error {
	if st == nil || st.Code() == codes.OK {
		return nil
	}

	code := grpcCodeToConnectCode(st.Code())
	connectErr := connect.NewError(code, errors.New(st.Message()))

	if proto := st.Proto(); proto != nil {
		for _, anyDetail := range proto.GetDetails() {
			if anyDetail == nil {
				continue
			}
			detail, err := connect.NewErrorDetail(anyDetail)
			if err != nil {
				continue
			}
			connectErr.AddDetail(detail)
		}
	}

	return connectErr
}

// ExtractErrorInfo 从 error 中提取并返回第一个附加的 *errdetails.ErrorInfo。
// 优先检索 *connect.Error（通过 errors.As 匹配，包括被包装的场景），
// 其次检索 gRPC *status.Status 错误（通过 status.FromError、错误链解包或 GRPCStatus 接口），
// 当 err 为 nil 或未包含 ErrorInfo 时返回 nil。
func ExtractErrorInfo(err error) *errdetails.ErrorInfo {
	if err == nil {
		return nil
	}

	// 1. 尝试从 *connect.Error 中提取
	var connectErr *connect.Error
	if errors.As(err, &connectErr) {
		for _, detail := range connectErr.Details() {
			msg, valErr := detail.Value()
			if valErr != nil {
				continue
			}
			if info, ok := msg.(*errdetails.ErrorInfo); ok {
				return info
			}
		}
	}

	// 2. 尝试从 gRPC status 错误链中提取（遍历解包）
	for curr := err; curr != nil; curr = errors.Unwrap(curr) {
		if st, ok := status.FromError(curr); ok && st.Code() != codes.OK {
			for _, detail := range st.Details() {
				if info, ok := detail.(*errdetails.ErrorInfo); ok {
					return info
				}
			}
			if proto := st.Proto(); proto != nil {
				for _, anyDetail := range proto.GetDetails() {
					if anyDetail == nil {
						continue
					}
					msg, unmarshalErr := anyDetail.UnmarshalNew()
					if unmarshalErr != nil {
						continue
					}
					if info, ok := msg.(*errdetails.ErrorInfo); ok {
						return info
					}
				}
			}
		}
	}

	// 3. 亦可通过 errors.As 匹配实现 GRPCStatus() *status.Status 契约的自定义包装错误
	var se interface{ GRPCStatus() *status.Status }
	if errors.As(err, &se) {
		st := se.GRPCStatus()
		if st != nil && st.Code() != codes.OK {
			for _, detail := range st.Details() {
				if info, ok := detail.(*errdetails.ErrorInfo); ok {
					return info
				}
			}
			if proto := st.Proto(); proto != nil {
				for _, anyDetail := range proto.GetDetails() {
					if anyDetail == nil {
						continue
					}
					msg, unmarshalErr := anyDetail.UnmarshalNew()
					if unmarshalErr != nil {
						continue
					}
					if info, ok := msg.(*errdetails.ErrorInfo); ok {
						return info
					}
				}
			}
		}
	}

	return nil
}
