package gonsensus

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestQuorum(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

	// Add delay after election before registration
	time.Sleep(1 * time.Second)

	if err := testCtx.registerAndVerifyObservers(ctx, manager); err != nil {
		t.Fatalf("failed to register and verify observers: %v", err)
	}

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
		TTL:          2 * time.Second,
		PollInterval: 500 * time.Millisecond,
		NodeID:       "test-node-1",
		LockPrefix:   "locks/",
		QuorumSize:   tc.quorumSize,
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

func (tc *quorumTestContext) registerObserverWithRetry(ctx context.Context, manager *Manager, nodeID string) error {
	for range 5 {
		if err := manager.RegisterObserver(ctx, nodeID, nil); err == nil {
			return nil
		}

		time.Sleep(100 * time.Millisecond)
	}

	return ErrFailedToRegisterObserver
}

func (tc *quorumTestContext) runHeartbeats(ctx context.Context, manager *Manager) {
	heartbeatCtx, stopHeartbeats := context.WithCancel(ctx)

	var heartbeatWg sync.WaitGroup

	tc.startObserverHeartbeats(heartbeatCtx, manager, &heartbeatWg)
	time.Sleep(3 * time.Second)

	// Verify with retries
	if err := tc.verifyObserverCount(ctx, manager, tc.quorumSize); err != nil {
		tc.t.Fatalf("failed to verify observers after heartbeats: %v", err)
	}

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

	ticker := time.NewTicker(100 * time.Millisecond)
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

func (tc *quorumTestContext) simulateQuorumLoss(ctx context.Context, manager *Manager) {
	lockInfo := tc.getLockInfo(ctx, manager)
	tc.markObserversInactive(lockInfo)
	tc.updateLockWithInactiveObservers(ctx, lockInfo)
	tc.waitForDemotion(ctx, manager)
}

func (tc *quorumTestContext) getLockInfo(ctx context.Context, manager *Manager) *LockInfo {
	lockInfo, err := manager.GetLockInfo(ctx)
	if err != nil {
		tc.t.Fatalf("%s: %v", ErrFailedToGetLockInfo, err)
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
		tc.t.Fatalf("%s: %v", ErrFailedToMarshalLockInfo, err)
	}

	_, err = tc.mockS3.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String("test-bucket"),
		Key:         aws.String("locks/leader"),
		Body:        bytes.NewReader(lockData),
		ContentType: aws.String(jsonContentType),
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

func (tc *quorumTestContext) registerAndVerifyObservers(ctx context.Context, manager *Manager) error {
	// Register observers one at a time with verification
	for iter := 1; iter <= tc.quorumSize; iter++ {
		nodeID := fmt.Sprintf("test-node-%d", iter)

		// Register with retry
		if err := tc.registerObserverWithRetry(ctx, manager, nodeID); err != nil {
			return fmt.Errorf("%w: %s", ErrFailedToRegisterObserver, nodeID)
		}

		// Verify after each registration
		if err := tc.verifyObserverCount(ctx, manager, iter); err != nil {
			return fmt.Errorf("%w: failed to verify observer count after registering %s", err, nodeID)
		}

		// Add delay between registrations
		time.Sleep(500 * time.Millisecond)
	}

	return nil
}

func (tc *quorumTestContext) verifyObserverCount(ctx context.Context, manager *Manager, expectedCount int) error {
	// Retry verification multiple times
	for iter := range 10 {
		activeCount, err := manager.GetActiveObservers(ctx)
		if err == nil && activeCount == expectedCount {
			tc.t.Logf("Successfully verified %d observers", activeCount)

			return nil
		}

		if err != nil {
			tc.t.Logf("Retry %d: Error getting active observers: %v", iter, err)
		} else {
			tc.t.Logf("Retry %d: Expected %d observers, got %d", iter, expectedCount, activeCount)
		}

		time.Sleep(200 * time.Millisecond)
	}

	return fmt.Errorf("%w", errFailedToVerifyObserverCount)
}
