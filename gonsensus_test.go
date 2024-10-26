package gonsensus

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// MockS3Client implements S3Client interface for testing
type MockS3Client struct {
	objects     map[string][]byte
	putError    error
	getError    error
	deleteError error
}

func NewMockS3Client() *MockS3Client {
	return &MockS3Client{
		objects: make(map[string][]byte),
	}
}

func (m *MockS3Client) PutObject(_ context.Context, params *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
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
	return &s3.PutObjectOutput{}, nil
}

func (m *MockS3Client) GetObject(_ context.Context, params *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if m.getError != nil {
		return nil, m.getError
	}

	key := aws.ToString(params.Key)
	data, exists := m.objects[key]
	if !exists {
		return nil, &types.NoSuchKey{}
	}

	return &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(data)),
	}, nil
}

func (m *MockS3Client) DeleteObject(_ context.Context, params *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	if m.deleteError != nil {
		return nil, m.deleteError
	}

	key := aws.ToString(params.Key)
	delete(m.objects, key)
	return &s3.DeleteObjectOutput{}, nil
}

func TestNewManager(t *testing.T) {
	tests := []struct {
		name        string
		client      S3Client
		bucket      string
		cfg         Config
		expectError bool
	}{
		{
			name:        "Valid configuration",
			client:      NewMockS3Client(),
			bucket:      "test-bucket",
			cfg:         Config{},
			expectError: false,
		},
		{
			name:        "Missing client",
			client:      nil,
			bucket:      "test-bucket",
			cfg:         Config{},
			expectError: true,
		},
		{
			name:        "Missing bucket",
			client:      NewMockS3Client(),
			bucket:      "",
			cfg:         Config{},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager, err := NewManager(tt.client, tt.bucket, tt.cfg)
			if tt.expectError {
				if err == nil {
					t.Error("expected error, got nil")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if manager == nil {
					t.Error("expected manager, got nil")
				}
			}
		})
	}
}

func TestAcquireLock(t *testing.T) {
	tests := []struct {
		name        string
		setupMock   func(*MockS3Client)
		expectError error
	}{
		{
			name: "Successful lock acquisition",
			setupMock: func(m *MockS3Client) {
				// Start with no lock
			},
			expectError: nil,
		},
		{
			name: "Lock exists and not expired",
			setupMock: func(m *MockS3Client) {
				lock := LockInfo{
					Node:      "other-node",
					Timestamp: time.Now(),
					Expiry:    time.Now().Add(30 * time.Second),
					Term:      1,
					Version:   "1",
				}
				data, _ := json.Marshal(lock)
				m.objects["locks/leader"] = data
			},
			expectError: ErrLockExists,
		},
		{
			name: "Lock exists but expired",
			setupMock: func(m *MockS3Client) {
				lock := LockInfo{
					Node:      "other-node",
					Timestamp: time.Now().Add(-60 * time.Second),
					Expiry:    time.Now().Add(-30 * time.Second),
					Term:      1,
					Version:   "1",
				}
				data, _ := json.Marshal(lock)
				m.objects["locks/leader"] = data
			},
			expectError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockS3 := NewMockS3Client()
			if tt.setupMock != nil {
				tt.setupMock(mockS3)
			}

			manager, err := NewManager(mockS3, "test-bucket", Config{
				TTL:        30 * time.Second,
				NodeID:     "test-node",
				LockPrefix: "locks/",
			})
			if err != nil {
				t.Fatalf("failed to create manager: %v", err)
			}

			err = manager.acquireLock(context.Background())
			if !errors.Is(err, tt.expectError) {
				t.Errorf("expected error %v, got %v", tt.expectError, err)
			}
		})
	}
}

func TestRenewLock(t *testing.T) {
	tests := []struct {
		name        string
		setupMock   func(*MockS3Client, *Manager)
		expectError error
	}{
		{
			name: "Successful renewal",
			setupMock: func(m *MockS3Client, mgr *Manager) {
				lock := LockInfo{
					Node:      mgr.nodeID,
					Timestamp: time.Now(),
					Expiry:    time.Now().Add(30 * time.Second),
					Term:      mgr.term,
					Version:   "1",
				}
				data, _ := json.Marshal(lock)
				m.objects[mgr.lockKey] = data
			},
			expectError: nil,
		},
		{
			name: "Lock not found",
			setupMock: func(m *MockS3Client, mgr *Manager) {
				// No lock exists
			},
			expectError: ErrLockNotFound,
		},
		{
			name: "Lock modified by other node",
			setupMock: func(m *MockS3Client, mgr *Manager) {
				lock := LockInfo{
					Node:      "other-node",
					Timestamp: time.Now(),
					Expiry:    time.Now().Add(30 * time.Second),
					Term:      mgr.term + 1,
					Version:   "2",
				}
				data, _ := json.Marshal(lock)
				m.objects[mgr.lockKey] = data
			},
			expectError: ErrLockModified,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockS3 := NewMockS3Client()
			manager, err := NewManager(mockS3, "test-bucket", Config{
				TTL:        30 * time.Second,
				NodeID:     "test-node",
				LockPrefix: "locks/",
			})
			if err != nil {
				t.Fatalf("failed to create manager: %v", err)
			}

			if tt.setupMock != nil {
				tt.setupMock(mockS3, manager)
			}

			err = manager.renewLock(context.Background())
			if !errors.Is(err, tt.expectError) {
				t.Errorf("expected error %v, got %v", tt.expectError, err)
			}
		})
	}
}

func TestLeaderCallbacks(t *testing.T) {
	mockS3 := NewMockS3Client()
	manager, err := NewManager(mockS3, "test-bucket", Config{
		TTL:          2 * time.Second,
		PollInterval: 500 * time.Millisecond,
		NodeID:       "test-node",
	})
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}

	electedCalled := false
	demotedCalled := false

	manager.SetCallbacks(
		func(ctx context.Context) error {
			electedCalled = true
			return nil
		},
		func(ctx context.Context) {
			demotedCalled = true
		},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Run the manager in a goroutine
	go func() {
		_ = manager.Run(ctx)
	}()

	// Wait for election
	time.Sleep(1 * time.Second)

	if !electedCalled {
		t.Error("expected election callback to be called")
	}

	// Simulate lock takeover by another node
	lock := LockInfo{
		Node:      "other-node",
		Timestamp: time.Now(),
		Expiry:    time.Now().Add(30 * time.Second),
		Term:      100,
		Version:   "other-1",
	}
	data, _ := json.Marshal(lock)
	mockS3.objects[manager.lockKey] = data

	// Wait for demotion
	time.Sleep(1 * time.Second)

	if !demotedCalled {
		t.Error("expected demotion callback to be called")
	}
}

func TestLockInfo(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name      string
		lockInfo  LockInfo
		wantValid bool
	}{
		{
			name: "Valid non-expired lock",
			lockInfo: LockInfo{
				Node:      "test-node",
				Timestamp: now,
				Expiry:    now.Add(30 * time.Second),
				Term:      1,
				Version:   "1",
			},
			wantValid: true,
		},
		{
			name: "Expired lock",
			lockInfo: LockInfo{
				Node:      "test-node",
				Timestamp: now.Add(-60 * time.Second),
				Expiry:    now.Add(-30 * time.Second),
				Term:      1,
				Version:   "1",
			},
			wantValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.lockInfo.IsValid(); got != tt.wantValid {
				t.Errorf("LockInfo.IsValid() = %v, want %v", got, tt.wantValid)
			}
		})
	}
}
