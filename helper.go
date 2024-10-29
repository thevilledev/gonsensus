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

// Helper function for error handling in marshal operations.
func must(data []byte, err error) []byte {
	if err != nil {
		panic(err)
	}

	return data
}
