package gonsensus

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"testing"
	"time"
)

var (
	errFailedToVerifyObserverCount = errors.New("failed to verify observer count after retries")
	errSimulated                   = errors.New("simulated error")
)

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

func setupTestManager() *Manager {
	return &Manager{
		s3Client:          NewMockS3Client(),
		bucket:            "test-bucket",
		lockKey:           "test-lock",
		ttl:               time.Second,
		nodeID:            "test-node",
		lease:             NewLease(), // Important: Initialize the lease
		onElected:         func(context.Context) error { return nil },
		onDemoted:         func(context.Context) {},
		requireQuorum:     true,
		requiredObservers: 3,
	}
}

func TestLeaderState_RaceConditions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fn   func(*testing.T, *leaderState)
	}{
		{
			name: "concurrent leader state changes",
			fn:   testConcurrentLeaderStateChanges,
		},
		{
			name: "concurrent maintenance and demotion",
			fn:   testConcurrentMaintenanceAndDemotion,
		},
		{
			name: "concurrent election attempts",
			fn:   testConcurrentElectionAttempts,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			state := &leaderState{
				manager: setupTestManager(),
			}
			testCase.fn(t, state)
		})
	}
}

func testConcurrentLeaderStateChanges(t *testing.T, state *leaderState) {
	t.Helper()

	var wGroup sync.WaitGroup

	iterations := 100 // Reduced for faster tests

	// Multiple goroutines changing leader state
	for range 5 {
		wGroup.Add(1)

		go func() {
			defer wGroup.Done()

			for range iterations {
				state.setLeader(true)
				_ = state.getIsLeader()
				state.setLeader(false)
			}
		}()
	}

	// Multiple goroutines reading leader state
	for range 5 {
		wGroup.Add(1)

		go func() {
			defer wGroup.Done()

			for range iterations {
				_ = state.getIsLeader()
			}
		}()
	}

	wGroup.Wait()
}

func testConcurrentMaintenanceAndDemotion(t *testing.T, state *leaderState) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	var wGroup sync.WaitGroup

	wGroup.Add(2)

	// Start maintenance loop
	go func() {
		defer wGroup.Done()
		state.setLeader(true)
		_ = state.runLeaderMaintenance(ctx)
	}()

	// Concurrent demotions
	go func() {
		defer wGroup.Done()

		ticker := time.NewTicker(10 * time.Millisecond)

		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				state.handleDemotion(ctx)
			}
		}
	}()

	wGroup.Wait()
}

func testConcurrentElectionAttempts(t *testing.T, state *leaderState) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	var wGroup sync.WaitGroup

	iterations := 10 // Reduced for faster tests

	// Multiple goroutines trying to become leader
	for range 5 {
		wGroup.Add(1)

		go func() {
			defer wGroup.Done()

			for range iterations {
				select {
				case <-ctx.Done():
					return
				default:
					_ = state.tryBecomeLeader(ctx)

					time.Sleep(time.Millisecond) // Add small delay to reduce contention
				}
			}
		}()
	}

	// Concurrent state checks
	wGroup.Add(1)

	go func() {
		defer wGroup.Done()

		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = state.getIsLeader()
			}
		}
	}()

	wGroup.Wait()
}

// Additional helper test to verify basic functionality.
func TestLeaderState_BasicOperations(t *testing.T) {
	t.Parallel()

	state := &leaderState{
		manager: setupTestManager(),
	}

	// Test basic leader state changes
	if state.getIsLeader() {
		t.Error("Expected initial state to be not leader")
	}

	state.setLeader(true)

	if !state.getIsLeader() {
		t.Error("Expected state to be leader after setting")
	}

	state.setLeader(false)

	if state.getIsLeader() {
		t.Error("Expected state to not be leader after unsetting")
	}
}
