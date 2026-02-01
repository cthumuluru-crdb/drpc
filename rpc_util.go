package drpc

import (
	"context"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ToRPCErr converts a drpc error to a gRPC status error for backward compatibility.
func ToRPCErr(err error) error {
	switch err {
	case nil, io.EOF:
		return err
	case context.DeadlineExceeded:
		return status.Error(codes.DeadlineExceeded, err.Error())
	case context.Canceled:
		return status.Error(codes.Canceled, err.Error())
	case io.ErrUnexpectedEOF:
		return status.Error(codes.Internal, err.Error())
	}
	if ConnectionError.Has(err) || ClosedError.Has(err) {
		return status.Error(codes.Unavailable, err.Error())
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	return status.Error(codes.Unknown, err.Error())
}
