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
