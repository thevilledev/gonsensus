package gonsensus

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

var (
	errSimulated = errors.New("simulated error")
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

func TestNewManager(t *testing.T) {
	t.Parallel()

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

	for _, tCase := range tests {
		t.Run(tCase.name, func(t *testing.T) {
			t.Parallel()

			manager, err := NewManager(tCase.client, tCase.bucket, tCase.cfg)

			if tCase.expectError {
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

func TestLeaderCallbacks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		setupMock     func(*MockS3Client)
		injectError   func(*MockS3Client)
		expectElected bool
		expectDemoted bool
	}{
		{
			name:          "Successfully becomes leader",
			setupMock:     nil, // Start with no lock
			injectError:   nil,
			expectElected: true,
			expectDemoted: false,
		},
		{
			name: "Fails to become leader due to existing lock",
			setupMock: func(mockClient *MockS3Client) {
				mockClient.mu.Lock()
				defer mockClient.mu.Unlock()

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
			injectError:   nil,
			expectElected: false,
			expectDemoted: false,
		},
		{
			name:      "Becomes leader but then loses leadership",
			setupMock: nil, // Start with no lock
			injectError: func(mockClient *MockS3Client) {
				mockClient.mu.Lock()
				defer mockClient.mu.Unlock()
				// Simulate failure during lock renewal
				mockClient.putError = errSimulated
			},
			expectElected: true,
			expectDemoted: true,
		},
	}

	for _, tCase := range tests {
		t.Run(tCase.name, func(t *testing.T) {
			t.Parallel()

			mockS3 := NewMockS3Client()
			if tCase.setupMock != nil {
				tCase.setupMock(mockS3)
			}

			elected := make(chan struct{}, 1)
			demoted := make(chan struct{}, 1)

			manager, err := NewManager(mockS3, "test-bucket", Config{
				TTL:          2 * time.Second, // Short TTL for testing
				PollInterval: 500 * time.Millisecond,
				NodeID:       "test-node",
				LockPrefix:   "locks/",
			})
			if err != nil {
				t.Fatalf("failed to create manager: %v", err)
			}

			manager.SetCallbacks(
				func(_ context.Context) error {
					elected <- struct{}{}

					return nil
				},
				func(_ context.Context) {
					demoted <- struct{}{}
				},
			)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// Start leadership monitoring in background
			var wGroup sync.WaitGroup

			wGroup.Add(1)

			go func() {
				defer wGroup.Done()

				err := manager.Run(ctx)
				if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("unexpected error from Run: %v", err)
				}
			}()

			// Wait for initial election attempt
			time.Sleep(1 * time.Second)

			// Inject error if configured
			if tCase.injectError != nil {
				tCase.injectError(mockS3)
			}

			// Wait for callbacks
			var gotElected, gotDemoted bool

			// Use select to check for callbacks
			checkCallbacks := func() {
				select {
				case <-elected:
					gotElected = true
				default:
				}

				select {
				case <-demoted:
					gotDemoted = true
				default:
				}
			}

			// Check multiple times over 3 seconds
			for range 6 {
				checkCallbacks()
				time.Sleep(500 * time.Millisecond)
			}

			// Cancel context and wait for cleanup
			cancel()
			wGroup.Wait()

			if gotElected != tCase.expectElected {
				t.Errorf("expected elected=%v, got %v", tCase.expectElected, gotElected)
			}

			if gotDemoted != tCase.expectDemoted {
				t.Errorf("expected demoted=%v, got %v", tCase.expectDemoted, gotDemoted)
			}
		})
	}
}

func TestQuorum(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	testCtx := &quorumTestContext{
		t:          t,
		mockS3:     NewMockS3Client(),
		quorumSize: 3,
		elected:    make(chan struct{}, 1),
		demoted:    make(chan struct{}, 1),
	}

	manager := testCtx.setupManager()

	var wGroup sync.WaitGroup

	testCtx.startManager(ctx, manager, &wGroup)
	testCtx.waitForElection()
	testCtx.registerObservers(ctx, manager)
	testCtx.verifyObservers(ctx, manager)
	testCtx.runHeartbeats(ctx, manager)
	testCtx.simulateQuorumLoss(ctx, manager)

	// Cleanup
	cancel()
	wGroup.Wait()
}

type quorumTestContext struct {
	t          *testing.T
	mockS3     *MockS3Client
	quorumSize int
	elected    chan struct{}
	demoted    chan struct{}
}

func (tc *quorumTestContext) setupManager() *Manager {
	manager, err := NewManager(tc.mockS3, "test-bucket", Config{
		TTL:           2 * time.Second,
		PollInterval:  500 * time.Millisecond,
		NodeID:        "test-node-1",
		LockPrefix:    "locks/",
		RequireQuorum: true,
		QuorumSize:    tc.quorumSize,
	})
	if err != nil {
		tc.t.Fatalf("failed to create manager: %v", err)
	}

	manager.SetCallbacks(
		func(_ context.Context) error {
			tc.elected <- struct{}{}

			return nil
		},
		func(_ context.Context) {
			tc.demoted <- struct{}{}
		},
	)

	return manager
}

func (tc *quorumTestContext) startManager(ctx context.Context, manager *Manager, wg *sync.WaitGroup) {
	wg.Add(1)

	go func() {
		defer wg.Done()

		err := manager.Run(ctx)
		if err != nil && !errors.Is(err, context.Canceled) &&
			!errors.Is(err, context.DeadlineExceeded) &&
			!errors.Is(err, ErrLostQuorum) {
			tc.t.Errorf("unexpected error from Run: %v", err)
		}
	}()
}

func (tc *quorumTestContext) waitForElection() {
	select {
	case <-tc.elected:
		log.Printf("DEBUG: Leader elected, proceeding with test")
	case <-time.After(3 * time.Second):
		tc.t.Fatal("leader election timed out")
	}
}

func (tc *quorumTestContext) registerObservers(ctx context.Context, manager *Manager) {
	for i := 1; i <= tc.quorumSize; i++ {
		nodeID := fmt.Sprintf("test-node-%d", i)
		if err := tc.registerObserverWithRetry(ctx, manager, nodeID); err != nil {
			tc.t.Fatalf("failed to register observer %s: %v", nodeID, err)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (tc *quorumTestContext) registerObserverWithRetry(ctx context.Context, manager *Manager, nodeID string) error {
	for range 5 {
		if err := manager.RegisterObserver(ctx, nodeID, nil); err == nil {
			return nil
		}

		time.Sleep(100 * time.Millisecond)
	}

	return ErrFailedToRegisterObserver
}

func (tc *quorumTestContext) verifyObservers(ctx context.Context, manager *Manager) {
	var activeCount int

	var err error

	for range 10 {
		activeCount, err = manager.GetActiveObservers(ctx)
		if err == nil && activeCount == tc.quorumSize {
			return
		}

		time.Sleep(200 * time.Millisecond)
	}

	if err != nil {
		tc.t.Fatalf("failed to verify observers: %v", err)
	}

	if activeCount != tc.quorumSize {
		tc.t.Errorf("expected %d active observers, got %d", tc.quorumSize, activeCount)
	}
}

func (tc *quorumTestContext) runHeartbeats(ctx context.Context, manager *Manager) {
	heartbeatCtx, stopHeartbeats := context.WithCancel(ctx)

	var heartbeatWg sync.WaitGroup

	tc.startObserverHeartbeats(heartbeatCtx, manager, &heartbeatWg)
	time.Sleep(2 * time.Second)
	tc.verifyInitialObservers(ctx, manager)

	stopHeartbeats()
	heartbeatWg.Wait()
}

func (tc *quorumTestContext) startObserverHeartbeats(ctx context.Context, manager *Manager, wGroup *sync.WaitGroup) {
	for i := 1; i <= tc.quorumSize; i++ {
		nodeID := fmt.Sprintf("test-node-%d", i)

		wGroup.Add(1)

		go tc.runObserverHeartbeat(ctx, manager, nodeID, wGroup)
	}
}

func (tc *quorumTestContext) runObserverHeartbeat(ctx context.Context, manager *Manager,
	nodeID string, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := manager.UpdateHeartbeat(ctx, nodeID); err != nil {
				if !errors.Is(err, context.Canceled) {
					tc.t.Logf("heartbeat update failed for %s: %v", nodeID, err)
				}
			}
		}
	}
}

func (tc *quorumTestContext) verifyInitialObservers(ctx context.Context, manager *Manager) {
	activeCount, err := manager.GetActiveObservers(ctx)
	if err != nil {
		tc.t.Fatalf("failed to get active observers: %v", err)
	}

	if activeCount != tc.quorumSize {
		tc.t.Errorf("expected %d active observers, got %d", tc.quorumSize, activeCount)
	}
}

func (tc *quorumTestContext) simulateQuorumLoss(ctx context.Context, manager *Manager) {
	lockInfo := tc.getLockInfo(ctx, manager)
	tc.markObserversInactive(lockInfo)
	tc.updateLockWithInactiveObservers(ctx, lockInfo)
	tc.waitForDemotion(ctx, manager)
}

func (tc *quorumTestContext) getLockInfo(ctx context.Context, manager *Manager) *LockInfo {
	lockInfo, err := manager.GetLockInfo(ctx)
	if err != nil {
		tc.t.Fatalf("failed to get lock info: %v", err)
	}

	return lockInfo
}

func (tc *quorumTestContext) markObserversInactive(lockInfo *LockInfo) {
	veryOldTime := time.Now().Add(-60 * time.Second)

	for nodeID := range lockInfo.Observers {
		observer := lockInfo.Observers[nodeID]
		observer.LastHeartbeat = veryOldTime
		observer.IsActive = false
		lockInfo.Observers[nodeID] = observer
	}
}

func (tc *quorumTestContext) updateLockWithInactiveObservers(ctx context.Context, lockInfo *LockInfo) {
	lockData, err := json.Marshal(lockInfo)
	if err != nil {
		tc.t.Fatalf("failed to marshal lock info: %v", err)
	}

	_, err = tc.mockS3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String("test-bucket"),
		Key:         aws.String("locks/leader"),
		Body:        bytes.NewReader(lockData),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		tc.t.Fatalf("failed to update lock info: %v", err)
	}
}

func (tc *quorumTestContext) waitForDemotion(ctx context.Context, manager *Manager) {
	select {
	case <-tc.demoted:
		log.Printf("DEBUG: Leader successfully demoted after losing quorum")
	case <-time.After(3 * time.Second):
		activeCount, _ := manager.GetActiveObservers(ctx)
		tc.t.Fatalf("leader failed to step down after losing quorum (active observers: %d)", activeCount)
	}
}
