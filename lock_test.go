package gonsensus

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"testing"
	"time"
)

func TestAcquireLock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setupMock   func(*MockS3Client)
		expectError error
	}{
		{
			name: "Successful lock acquisition",
			setupMock: func(_ *MockS3Client) {
				// Start with no lock
			},
			expectError: nil,
		},
		{
			name: "Lock exists and not expired",
			setupMock: func(mockClient *MockS3Client) {
				lock := LockInfo{
					Node:      "other-node",
					Timestamp: time.Now(),
					Expiry:    time.Now().Add(30 * time.Second),
					Term:      1,
					Version:   "1",
				}
				data, err := json.Marshal(lock)
				if err != nil {
					log.Panic("mock setup fail")
				}
				mockClient.objects["locks/leader"] = data
			},
			expectError: ErrLockExists,
		},
		{
			name: "Lock exists but expired",
			setupMock: func(mockClient *MockS3Client) {
				lock := LockInfo{
					Node:      "other-node",
					Timestamp: time.Now().Add(-60 * time.Second),
					Expiry:    time.Now().Add(-30 * time.Second),
					Term:      1,
					Version:   "1",
				}
				data, err := json.Marshal(lock)
				if err != nil {
					log.Panic("mock setup fail")
				}
				mockClient.objects["locks/leader"] = data
			},
			expectError: nil,
		},
	}

	for _, tCase := range tests {
		t.Run(tCase.name, func(t *testing.T) {
			t.Parallel()

			mockS3 := NewMockS3Client()

			if tCase.setupMock != nil {
				tCase.setupMock(mockS3)
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
			if !errors.Is(err, tCase.expectError) {
				t.Errorf("expected error %v, got %v", tCase.expectError, err)
			}
		})
	}
}

func TestRenewLock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setupMock   func(*MockS3Client, *Manager)
		expectError error
	}{
		{
			name: "Successful renewal",
			setupMock: func(mockClient *MockS3Client, mgr *Manager) {
				lock := LockInfo{
					Node:      mgr.nodeID,
					Timestamp: time.Now(),
					Expiry:    time.Now().Add(30 * time.Second),
					Term:      mgr.getCurrentTerm(),
					Version:   "1",
				}

				// Initialize the lease with the current lock info
				mgr.lease.UpdateLease(&lock)

				data, err := json.Marshal(lock)
				if err != nil {
					log.Panic("mock setup fail")
				}
				mockClient.objects[mgr.lockKey] = data
			},
			expectError: nil,
		},
		{
			name: "Lock not found",
			setupMock: func(_ *MockS3Client, _ *Manager) {
				// No lock exists
			},
			expectError: ErrLockNotFound,
		},
		{
			name: "Lock modified by other node",
			setupMock: func(mockClient *MockS3Client, mgr *Manager) {
				originalLock := LockInfo{
					Node:      "other-node",
					Timestamp: time.Now(),
					Expiry:    time.Now().Add(30 * time.Second),
					Term:      mgr.incrementTerm(),
					Version:   "1",
				}

				mgr.lease.UpdateLease(&originalLock)

				// Then simulate modification by another node
				modifiedLock := LockInfo{
					Node:      "other-node",
					Timestamp: time.Now(),
					Expiry:    time.Now().Add(30 * time.Second),
					Term:      mgr.incrementTerm(),
					Version:   "2",
				}

				data, err := json.Marshal(modifiedLock)
				if err != nil {
					log.Panic("mock setup fail")
				}
				mockClient.objects[mgr.lockKey] = data
			},
			expectError: ErrLockModified,
		},
	}

	for _, tCase := range tests {
		t.Run(tCase.name, func(t *testing.T) {
			t.Parallel()

			mockS3 := NewMockS3Client()
			manager, err := NewManager(mockS3, "test-bucket", Config{
				TTL:        30 * time.Second,
				NodeID:     "test-node",
				LockPrefix: "locks/",
			})

			if err != nil {
				t.Fatalf("failed to create manager: %v", err)
			}

			if tCase.setupMock != nil {
				tCase.setupMock(mockS3, manager)
			}

			err = manager.renewLock(context.Background())
			if !errors.Is(err, tCase.expectError) {
				t.Errorf("expected error %v, got %v", tCase.expectError, err)
			}
		})
	}
}

func TestLockInfo(t *testing.T) {
	t.Parallel()

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

	for _, tCase := range tests {
		t.Run(tCase.name, func(t *testing.T) {
			t.Parallel()

			if got := tCase.lockInfo.IsValid(); got != tCase.wantValid {
				t.Errorf("LockInfo.IsValid() = %v, want %v", got, tCase.wantValid)
			}
		})
	}
}
