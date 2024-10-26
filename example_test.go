// example_test.go
package gonsensus_test

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/thevilledev/gonsensus"
)

func Example() {
	// Load AWS configuration
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Printf("Failed to load AWS config: %v", err)
		return
	}

	// Create S3 client
	s3Client := s3.NewFromConfig(cfg)

	// Create consensus manager config
	consensusConfig := gonsensus.Config{
		TTL:          30 * time.Second,
		PollInterval: 5 * time.Second,
		LockPrefix:   "consensus/",
		NodeID:       "example-node-1",
	}

	// Create consensus manager
	manager, err := gonsensus.NewManager(s3Client, "my-bucket-1", consensusConfig)
	if err != nil {
		log.Printf("Failed to create consensus manager: %v", err)
		return
	}

	// Create worker to handle leader duties
	worker := &Worker{nodeID: "example-node-1"}

	// Set callbacks
	manager.SetCallbacks(worker.OnElected, worker.OnDemoted)

	// Create cancellable context
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	// Run consensus manager
	go func() {
		if err := manager.Run(ctx); err != nil && err != context.Canceled {
			fmt.Printf("Consensus manager error: %v\n", err)
		}
	}()

	// Simulate some work
	time.Sleep(2 * time.Second)

	// Get current leader info
	lockInfo, err := manager.GetLockInfo(ctx)
	if err != nil {
		fmt.Printf("Failed to get lock info: %v\n", err)
		return
	}

	fmt.Printf("Current leader: %s (Term: %d)\n", lockInfo.Node, lockInfo.Term)

	// Output:
	// Node example-node-1 elected as leader
	// Current leader: example-node-1 (Term: 1)
}

// Worker handles leader-specific tasks
type Worker struct {
	nodeID   string
	isLeader bool
	mu       sync.RWMutex
}

func (w *Worker) OnElected(ctx context.Context) error {
	w.mu.Lock()
	w.isLeader = true
	w.mu.Unlock()

	fmt.Printf("Node %s elected as leader\n", w.nodeID)
	return nil
}

func (w *Worker) OnDemoted(ctx context.Context) {
	w.mu.Lock()
	w.isLeader = false
	w.mu.Unlock()

	fmt.Printf("Node %s demoted from leader\n", w.nodeID)
}
