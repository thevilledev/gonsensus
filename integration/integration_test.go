// Package integration_test contains integration tests for gonsensus
package integration_test

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/thevilledev/gonsensus"
)

// TestThreeNodeQuorum_Integration verifies quorum behavior with three nodes
func TestThreeNodeQuorum_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Load AWS config
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		t.Fatalf("Failed to load AWS config: %v", err)
	}

	// Create S3 client
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		// https://github.com/aws/aws-sdk-go-v2/discussions/2960
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	})

	// Create unique test ID to prevent interference between test runs
	testID := fmt.Sprintf("%d", time.Now().UnixNano())
	bucket := "my-bucket-1"
	lockPrefix := fmt.Sprintf("integration-test/%s/", testID)

	// Create test context for synchronization
	testCtx := &testContext{
		t:           t,
		leaderChan:  make(chan string, 1),
		demoteChan:  make(chan string, 1),
		leaderMu:    &sync.RWMutex{},
		currentTerm: 0,
	}

	// Create three nodes with staggered start delays
	nodes := make([]*testNode, 3)
	for i := range nodes {
		nodes[i] = newTestNode(t, s3Client, fmt.Sprintf("node-%d", i+1), testCtx, bucket, lockPrefix)
	}

	// Start nodes with staggered delays
	var wg sync.WaitGroup
	for i, node := range nodes {
		wg.Add(1)
		go func(n *testNode, delay time.Duration) {
			defer wg.Done()
			time.Sleep(delay)
			n.start(ctx)
		}(node, time.Duration(i)*500*time.Millisecond)
	}

	// Wait for initial leader election
	var leaderID string
	select {
	case leaderID = <-testCtx.leaderChan:
		t.Logf("Node %s became initial leader", leaderID)
	case <-time.After(5 * time.Second):
		t.Fatal("Timed out waiting for leader election")
	}

	// Wait for leader to stabilize
	time.Sleep(2 * time.Second)

	// Register observers sequentially
	for _, node := range nodes {
		for i := 0; i < 3; i++ { // Retry loop
			err := node.manager.RegisterObserver(ctx, node.nodeID, nil)
			if err == nil {
				t.Logf("Successfully registered node %s as observer", node.nodeID)
				break
			}
			if i == 2 {
				t.Fatalf("Failed to register node %s after retries: %v", node.nodeID, err)
			}
			time.Sleep(time.Second)
		}
	}

	// Start heartbeats after all registrations
	for _, node := range nodes {
		wg.Add(1)
		go func(n *testNode) {
			defer wg.Done()
			n.runHeartbeat(ctx)
		}(node)
	}

	// Wait for heartbeats to establish
	time.Sleep(3 * time.Second)

	// Verify leader and quorum
	if err := testCtx.verifyLeaderAndQuorum(ctx, nodes); err != nil {
		t.Error(err)
	}

	// Clean shutdown
	cancel()
	wg.Wait()
}

type testContext struct {
	t           *testing.T
	leaderChan  chan string
	demoteChan  chan string
	leaderMu    *sync.RWMutex
	currentTerm int64
}

func (tc *testContext) verifyLeaderAndQuorum(ctx context.Context, nodes []*testNode) error {
	leaders := countLeaders(nodes)
	if leaders != 1 {
		return fmt.Errorf("expected exactly 1 leader, got %d", leaders)
	}

	activeNodes := countActiveNodes(ctx, nodes)
	if activeNodes != 3 {
		return fmt.Errorf("expected 3 active nodes, got %d", activeNodes)
	}

	return nil
}

// testNode represents a node in the integration test
type testNode struct {
	t        *testing.T
	manager  *gonsensus.Manager
	nodeID   string
	isLeader bool
	mu       sync.RWMutex
	ctx      *testContext
}

func newTestNode(t *testing.T, s3Client *s3.Client, nodeID string, tc *testContext, bucket, lockPrefix string) *testNode {
	node := &testNode{
		t:      t,
		nodeID: nodeID,
		ctx:    tc,
	}

	manager, err := gonsensus.NewManager(s3Client, bucket, gonsensus.Config{
		TTL:          10 * time.Second, // Increased TTL
		PollInterval: 2 * time.Second,  // Increased poll interval
		NodeID:       nodeID,
		LockPrefix:   lockPrefix,
		QuorumSize:   3,
		GracePeriod:  5 * time.Second, // Increased grace period
	})
	if err != nil {
		t.Fatalf("Failed to create manager for node %s: %v", nodeID, err)
	}

	manager.SetCallbacks(
		node.onElected,
		node.onDemoted,
	)

	node.manager = manager
	return node
}

func (n *testNode) start(ctx context.Context) {
	if err := n.manager.Run(ctx); err != nil && err != context.Canceled {
		n.t.Logf("Node %s stopped with error: %v", n.nodeID, err)
	}
}

func (n *testNode) onElected(ctx context.Context) error {
	n.mu.Lock()
	n.isLeader = true
	n.mu.Unlock()

	select {
	case n.ctx.leaderChan <- n.nodeID:
	default:
	}

	log.Printf("Node %s became leader", n.nodeID)
	return nil
}

func (n *testNode) onDemoted(ctx context.Context) {
	n.mu.Lock()
	n.isLeader = false
	n.mu.Unlock()

	select {
	case n.ctx.demoteChan <- n.nodeID:
	default:
	}

	log.Printf("Node %s was demoted", n.nodeID)
}

func countLeaders(nodes []*testNode) int {
	leaders := 0
	for _, node := range nodes {
		node.mu.RLock()
		if node.isLeader {
			leaders++
		}
		node.mu.RUnlock()
	}
	return leaders
}

func countActiveNodes(ctx context.Context, nodes []*testNode) int {
	active := 0
	for _, node := range nodes {
		lockInfo, err := node.manager.GetLockInfo(ctx)
		if err != nil {
			continue
		}
		if lockInfo.Observers != nil {
			active = len(lockInfo.Observers)
		}
	}
	return active
}

func (n *testNode) runHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	retryCount := 0
	maxRetries := 3

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := n.manager.UpdateHeartbeat(ctx, n.nodeID)
			if err != nil {
				retryCount++
				if retryCount >= maxRetries {
					n.t.Logf("Failed to update heartbeat for node %s after %d retries: %v", n.nodeID, retryCount, err)
					return
				}
				continue
			}
			retryCount = 0
		}
	}
}
