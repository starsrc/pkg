package serverx_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/starsrc/pkg/observability/slogx"
	"github.com/starsrc/pkg/serverx"
)

func TestStatusToConnectError(t *testing.T) {
	t.Run("nil status returns nil", func(t *testing.T) {
		err := serverx.StatusToConnectError(nil)
		assert.NoError(t, err)
	})

	t.Run("status with code OK returns nil", func(t *testing.T) {
		st := status.New(codes.OK, "ok")
		err := serverx.StatusToConnectError(st)
		assert.NoError(t, err)
	})

	t.Run("all 17 gRPC status codes map 1:1 to connect codes", func(t *testing.T) {
		testCases := []struct {
			grpcCode    codes.Code
			connectCode connect.Code
		}{
			{codes.OK, 0},
			{codes.Canceled, connect.CodeCanceled},
			{codes.Unknown, connect.CodeUnknown},
			{codes.InvalidArgument, connect.CodeInvalidArgument},
			{codes.DeadlineExceeded, connect.CodeDeadlineExceeded},
			{codes.NotFound, connect.CodeNotFound},
			{codes.AlreadyExists, connect.CodeAlreadyExists},
			{codes.PermissionDenied, connect.CodePermissionDenied},
			{codes.ResourceExhausted, connect.CodeResourceExhausted},
			{codes.FailedPrecondition, connect.CodeFailedPrecondition},
			{codes.Aborted, connect.CodeAborted},
			{codes.OutOfRange, connect.CodeOutOfRange},
			{codes.Unimplemented, connect.CodeUnimplemented},
			{codes.Internal, connect.CodeInternal},
			{codes.Unavailable, connect.CodeUnavailable},
			{codes.DataLoss, connect.CodeDataLoss},
			{codes.Unauthenticated, connect.CodeUnauthenticated},
		}

		for _, tc := range testCases {
			st := status.New(tc.grpcCode, "error message for "+tc.grpcCode.String())
			err := serverx.StatusToConnectError(st)
			if tc.grpcCode == codes.OK {
				assert.NoError(t, err)
				continue
			}
			require.Error(t, err)
			var connectErr *connect.Error
			require.True(t, connect.CodeOf(err) == tc.connectCode, "expected connect code %v, got %v", tc.connectCode, connect.CodeOf(err))
			require.ErrorAs(t, err, &connectErr)
			assert.Equal(t, tc.connectCode, connectErr.Code())
			assert.Contains(t, connectErr.Message(), "error message for "+tc.grpcCode.String())
		}
	})

	t.Run("unknown gRPC status code maps to CodeUnknown", func(t *testing.T) {
		st := status.New(codes.Code(999), "unknown custom code")
		err := serverx.StatusToConnectError(st)
		require.Error(t, err)
		assert.Equal(t, connect.CodeUnknown, connect.CodeOf(err))
	})

	t.Run("preserves details", func(t *testing.T) {
		st := status.New(codes.InvalidArgument, "invalid input")
		info := &errdetails.ErrorInfo{
			Reason:   "INVALID_ARGUMENT",
			Domain:   "starsrc.test",
			Metadata: map[string]string{"field": "email"},
		}
		stWithDetails, err := st.WithDetails(info)
		require.NoError(t, err)

		connectErr := serverx.StatusToConnectError(stWithDetails)
		require.Error(t, connectErr)

		var cErr *connect.Error
		require.ErrorAs(t, connectErr, &cErr)
		require.Len(t, cErr.Details(), 1)

		msg, err := cErr.Details()[0].Value()
		require.NoError(t, err)
		extractedInfo, ok := msg.(*errdetails.ErrorInfo)
		require.True(t, ok)
		assert.Equal(t, "INVALID_ARGUMENT", extractedInfo.GetReason())
		assert.Equal(t, "starsrc.test", extractedInfo.GetDomain())
		assert.Equal(t, "email", extractedInfo.GetMetadata()["field"])
	})

	t.Run("preserves multiple heterogeneous details in order", func(t *testing.T) {
		st := status.New(codes.InvalidArgument, "invalid input with multiple details")
		info := &errdetails.ErrorInfo{
			Reason:   "INVALID_ARGUMENT",
			Domain:   "starsrc.test",
			Metadata: map[string]string{"field": "email"},
		}
		badReq := &errdetails.BadRequest{
			FieldViolations: []*errdetails.BadRequest_FieldViolation{
				{
					Field:       "email",
					Description: "must be a valid email address",
				},
			},
		}
		retryInfo := &errdetails.RetryInfo{
			RetryDelay: durationpb.New(5 * time.Second),
		}

		stWithDetails, err := st.WithDetails(info, badReq, retryInfo)
		require.NoError(t, err)

		connectErr := serverx.StatusToConnectError(stWithDetails)
		require.Error(t, connectErr)

		var cErr *connect.Error
		require.ErrorAs(t, connectErr, &cErr)
		require.Len(t, cErr.Details(), 3)

		// 1. ErrorInfo
		msg0, err := cErr.Details()[0].Value()
		require.NoError(t, err)
		extractedInfo, ok := msg0.(*errdetails.ErrorInfo)
		require.True(t, ok)
		assert.Equal(t, "INVALID_ARGUMENT", extractedInfo.GetReason())
		assert.Equal(t, "starsrc.test", extractedInfo.GetDomain())
		assert.Equal(t, "email", extractedInfo.GetMetadata()["field"])

		// 2. BadRequest
		msg1, err := cErr.Details()[1].Value()
		require.NoError(t, err)
		extractedBadReq, ok := msg1.(*errdetails.BadRequest)
		require.True(t, ok)
		require.Len(t, extractedBadReq.GetFieldViolations(), 1)
		assert.Equal(t, "email", extractedBadReq.GetFieldViolations()[0].GetField())
		assert.Equal(t, "must be a valid email address", extractedBadReq.GetFieldViolations()[0].GetDescription())

		// 3. RetryInfo
		msg2, err := cErr.Details()[2].Value()
		require.NoError(t, err)
		extractedRetry, ok := msg2.(*errdetails.RetryInfo)
		require.True(t, ok)
		assert.Equal(t, int64(5), extractedRetry.GetRetryDelay().GetSeconds())
	})
}

func TestExtractErrorInfo(t *testing.T) {
	sampleInfo := &errdetails.ErrorInfo{
		Reason:   "RESOURCE_LOCKED",
		Domain:   "starsrc.test",
		Metadata: map[string]string{"resource_id": "res-123"},
	}

	t.Run("nil error returns nil", func(t *testing.T) {
		assert.Nil(t, serverx.ExtractErrorInfo(nil))
	})

	t.Run("plain error without details returns nil", func(t *testing.T) {
		assert.Nil(t, serverx.ExtractErrorInfo(assert.AnError))
	})

	t.Run("connect error without details returns nil", func(t *testing.T) {
		cErr := connect.NewError(connect.CodeNotFound, assert.AnError)
		assert.Nil(t, serverx.ExtractErrorInfo(cErr))
	})

	t.Run("connect error with ErrorInfo detail", func(t *testing.T) {
		cErr := connect.NewError(connect.CodeAborted, assert.AnError)
		detail, err := connect.NewErrorDetail(sampleInfo)
		require.NoError(t, err)
		cErr.AddDetail(detail)

		extracted := serverx.ExtractErrorInfo(cErr)
		require.NotNil(t, extracted)
		assert.Equal(t, "RESOURCE_LOCKED", extracted.GetReason())
		assert.Equal(t, "starsrc.test", extracted.GetDomain())
		assert.Equal(t, "res-123", extracted.GetMetadata()["resource_id"])
	})

	t.Run("wrapped connect error with ErrorInfo detail", func(t *testing.T) {
		cErr := connect.NewError(connect.CodeAborted, assert.AnError)
		detail, err := connect.NewErrorDetail(sampleInfo)
		require.NoError(t, err)
		cErr.AddDetail(detail)

		wrapped := fmt.Errorf("outer wrapper: %w", cErr)

		extracted := serverx.ExtractErrorInfo(wrapped)
		require.NotNil(t, extracted)
		assert.Equal(t, "RESOURCE_LOCKED", extracted.GetReason())
		assert.Equal(t, "starsrc.test", extracted.GetDomain())
	})

	t.Run("gRPC error with ErrorInfo detail", func(t *testing.T) {
		st := status.New(codes.Aborted, "transaction aborted")
		stWithDetails, err := st.WithDetails(sampleInfo)
		require.NoError(t, err)

		grpcErr := stWithDetails.Err()
		extracted := serverx.ExtractErrorInfo(grpcErr)
		require.NotNil(t, extracted)
		assert.Equal(t, "RESOURCE_LOCKED", extracted.GetReason())
		assert.Equal(t, "starsrc.test", extracted.GetDomain())
	})

	t.Run("wrapped gRPC error with ErrorInfo detail", func(t *testing.T) {
		st := status.New(codes.Aborted, "transaction aborted")
		stWithDetails, err := st.WithDetails(sampleInfo)
		require.NoError(t, err)

		grpcErr := stWithDetails.Err()
		wrapped := fmt.Errorf("wrapped outer: %w", fmt.Errorf("wrapped inner: %w", grpcErr))

		extracted := serverx.ExtractErrorInfo(wrapped)
		require.NotNil(t, extracted)
		assert.Equal(t, "RESOURCE_LOCKED", extracted.GetReason())
		assert.Equal(t, "starsrc.test", extracted.GetDomain())
	})

	t.Run("detail without ErrorInfo returns nil", func(t *testing.T) {
		cErr := connect.NewError(connect.CodeInvalidArgument, assert.AnError)
		badReq := &errdetails.BadRequest{
			FieldViolations: []*errdetails.BadRequest_FieldViolation{
				{Field: "username", Description: "too short"},
			},
		}
		detail, err := connect.NewErrorDetail(badReq)
		require.NoError(t, err)
		cErr.AddDetail(detail)

		assert.Nil(t, serverx.ExtractErrorInfo(cErr))
	})

	t.Run("gRPC error without details returns nil", func(t *testing.T) {
		st := status.New(codes.NotFound, "entity not found")
		assert.Nil(t, serverx.ExtractErrorInfo(st.Err()))
	})

	t.Run("gRPC error with non-ErrorInfo detail returns nil", func(t *testing.T) {
		st := status.New(codes.InvalidArgument, "invalid input")
		badReq := &errdetails.BadRequest{
			FieldViolations: []*errdetails.BadRequest_FieldViolation{
				{Field: "username", Description: "too short"},
			},
		}
		stWithDetails, err := st.WithDetails(badReq)
		require.NoError(t, err)
		assert.Nil(t, serverx.ExtractErrorInfo(stWithDetails.Err()))
	})

	t.Run("custom error type implementing GRPCStatus interface", func(t *testing.T) {
		st := status.New(codes.ResourceExhausted, "quota exceeded")
		stWithDetails, err := st.WithDetails(sampleInfo)
		require.NoError(t, err)

		custom := &customGRPCStatusError{st: stWithDetails}
		extracted := serverx.ExtractErrorInfo(custom)
		require.NotNil(t, extracted)
		assert.Equal(t, "RESOURCE_LOCKED", extracted.GetReason())
	})

	t.Run("multiple details extracts first ErrorInfo", func(t *testing.T) {
		firstInfo := &errdetails.ErrorInfo{Reason: "FIRST_REASON", Domain: "starsrc.test"}
		secondInfo := &errdetails.ErrorInfo{Reason: "SECOND_REASON", Domain: "starsrc.test"}

		cErr := connect.NewError(connect.CodeInternal, assert.AnError)
		d1, err := connect.NewErrorDetail(firstInfo)
		require.NoError(t, err)
		d2, err := connect.NewErrorDetail(secondInfo)
		require.NoError(t, err)

		cErr.AddDetail(d1)
		cErr.AddDetail(d2)

		extracted := serverx.ExtractErrorInfo(cErr)
		require.NotNil(t, extracted)
		assert.Equal(t, "FIRST_REASON", extracted.GetReason())
	})
}

type customGRPCStatusError struct {
	st *status.Status
}

func (e *customGRPCStatusError) Error() string {
	if e.st != nil {
		return e.st.Message()
	}
	return "custom status error"
}

func (e *customGRPCStatusError) GRPCStatus() *status.Status {
	return e.st
}

func TestMaskInterceptorDetailPreservation(t *testing.T) {
	sampleInfo := &errdetails.ErrorInfo{
		Reason:   "INTERNAL_DATABASE_FAIL",
		Domain:   "starsrc.test",
		Metadata: map[string]string{"shard": "shard-1"},
	}

	testCases := []struct {
		name         string
		sourceErr    func() error
		expectedCode connect.Code
		masked       bool
	}{
		{
			name: "Connect CodeInternal with ErrorInfo is masked but preserves ErrorInfo",
			sourceErr: func() error {
				cErr := connect.NewError(connect.CodeInternal, fmt.Errorf("sensitive db connection lost"))
				detail, err := connect.NewErrorDetail(sampleInfo)
				require.NoError(t, err)
				cErr.AddDetail(detail)
				return cErr
			},
			expectedCode: connect.CodeInternal,
			masked:       true,
		},
		{
			name: "Connect CodeUnknown with ErrorInfo is masked but preserves ErrorInfo",
			sourceErr: func() error {
				cErr := connect.NewError(connect.CodeUnknown, fmt.Errorf("unhandled panic trace"))
				detail, err := connect.NewErrorDetail(sampleInfo)
				require.NoError(t, err)
				cErr.AddDetail(detail)
				return cErr
			},
			expectedCode: connect.CodeInternal,
			masked:       true,
		},
		{
			name: "StatusToConnectError with Internal code passes through MaskInterceptor preserving ErrorInfo",
			sourceErr: func() error {
				st := status.New(codes.Internal, "underlying internal error")
				stWithDetails, err := st.WithDetails(sampleInfo)
				require.NoError(t, err)
				return serverx.StatusToConnectError(stWithDetails)
			},
			expectedCode: connect.CodeInternal,
			masked:       true,
		},
		{
			name: "Connect CodePermissionDenied with ErrorInfo preserves code and ErrorInfo without masking",
			sourceErr: func() error {
				cErr := connect.NewError(connect.CodePermissionDenied, fmt.Errorf("forbidden action"))
				detail, err := connect.NewErrorDetail(sampleInfo)
				require.NoError(t, err)
				cErr.AddDetail(detail)
				return cErr
			},
			expectedCode: connect.CodePermissionDenied,
			masked:       false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			maskInterceptor := slogx.NewMaskInterceptor(nil)

			unary := maskInterceptor.WrapUnary(func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
				return nil, tc.sourceErr()
			})

			_, err := unary(context.Background(), nil)
			require.Error(t, err)

			var connectErr *connect.Error
			require.ErrorAs(t, err, &connectErr)
			assert.Equal(t, tc.expectedCode, connectErr.Code())

			if tc.masked {
				assert.Equal(t, "internal server error", connectErr.Message())
			} else {
				assert.Equal(t, "forbidden action", connectErr.Message())
			}

			extracted := serverx.ExtractErrorInfo(err)
			require.NotNil(t, extracted, "ErrorInfo must be preserved across MaskInterceptor")
			assert.Equal(t, "INTERNAL_DATABASE_FAIL", extracted.GetReason())
			assert.Equal(t, "starsrc.test", extracted.GetDomain())
			assert.Equal(t, "shard-1", extracted.GetMetadata()["shard"])
		})
	}
}
