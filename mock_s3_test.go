package gonsensus

import (
	"bytes"
	"context"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// MockS3Client implements S3Client interface for testing.
type MockS3Client struct {
	objects     map[string][]byte
	putError    error
	getError    error
	deleteError error
	mu          sync.Mutex
	putCount    int
	getCount    int
}

func NewMockS3Client() *MockS3Client {
	return &MockS3Client{
		objects: make(map[string][]byte),
	}
}

func (m *MockS3Client) PutObject(_ context.Context, params *s3.PutObjectInput,
	_ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.putError != nil {
		return nil, m.putError
	}

	key := aws.ToString(params.Key)

	// Handle IfNoneMatch condition
	if params.IfNoneMatch != nil && *params.IfNoneMatch == "*" {
		if _, exists := m.objects[key]; exists {
			return nil, &smithy.GenericAPIError{
				Code:    "PreconditionFailed",
				Message: "Object already exists",
			}
		}
	}

	data, err := io.ReadAll(params.Body)
	if err != nil {
		return nil, err
	}

	m.objects[key] = data
	m.putCount++

	return &s3.PutObjectOutput{}, nil
}

func (m *MockS3Client) GetObject(_ context.Context, params *s3.GetObjectInput,
	_ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.getError != nil {
		return nil, m.getError
	}

	key := aws.ToString(params.Key)
	data, exists := m.objects[key]

	m.getCount++

	if !exists {
		return nil, &types.NoSuchKey{}
	}

	return &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(data)),
	}, nil
}

func (m *MockS3Client) DeleteObject(_ context.Context, params *s3.DeleteObjectInput,
	_ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.deleteError != nil {
		return nil, m.deleteError
	}

	key := aws.ToString(params.Key)
	delete(m.objects, key)

	return &s3.DeleteObjectOutput{}, nil
}
