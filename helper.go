package gonsensus

import (
	"errors"

	"github.com/aws/smithy-go"
)

// Helper function to check AWS error codes.
func isAWSErrorCode(err error, code string) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode() == code
	}

	return false
}
