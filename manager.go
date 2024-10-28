package gonsensus

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

type Manager struct {
	s3Client     S3Client
	bucket       string
	nodeID       string
	lockKey      string
	ttl          time.Duration
	term         atomic.Int64
	pollInterval time.Duration

	callbackMu sync.RWMutex
	onElected  func(context.Context) error
	onDemoted  func(context.Context)

	lease *Lease

	requireQuorum     bool
	requiredObservers int

	gracePeriod time.Duration
}

type observerInfo struct {
	LastHeartbeat time.Time         `json:"lastHeartbeat"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	IsActive      bool              `json:"isActive"`
}

func NewManager(client S3Client, bucket string, cfg Config) (*Manager, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: S3 client is required", ErrInvalidConfig)
	}

	if bucket == "" {
		return nil, fmt.Errorf("%w: bucket name is required", ErrInvalidConfig)
	}

	nodeID := cfg.NodeID
	if nodeID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			hostname = "unknown"
		}

		nodeID = fmt.Sprintf("node-%s-%d", hostname, time.Now().UnixNano())
	}

	if cfg.TTL == 0 {
		cfg.TTL = defaultTTL
	}

	lockPrefix := cfg.LockPrefix
	if lockPrefix == "" {
		lockPrefix = "locks/"
	}

	if cfg.PollInterval == 0 {
		cfg.PollInterval = defaultPollInterval
	}

	if cfg.GracePeriod == 0 {
		cfg.GracePeriod = cfg.TTL / defaultGracePeriodDivider
	}

	if cfg.QuorumSize == 0 {
		cfg.QuorumSize = defaultQuorumSize
	}

	return &Manager{
		s3Client:          client,
		bucket:            bucket,
		nodeID:            nodeID,
		lockKey:           lockPrefix + "leader",
		ttl:               cfg.TTL,
		pollInterval:      cfg.PollInterval,
		lease:             NewLease(),
		requireQuorum:     cfg.RequireQuorum,
		gracePeriod:       cfg.GracePeriod,
		requiredObservers: cfg.QuorumSize,
	}, nil
}

func (m *Manager) SetCallbacks(onElected func(context.Context) error, onDemoted func(context.Context)) {
	m.callbackMu.Lock()
	defer m.callbackMu.Unlock()

	m.onElected = onElected
	m.onDemoted = onDemoted
}

// Thread-safe term management.
func (m *Manager) incrementTerm() int64 {
	return m.term.Add(1)
}

func (m *Manager) getCurrentTerm() int64 {
	return m.term.Load()
}

// Check if lock is expired and try to acquire if it is.
func (m *Manager) acquireLock(ctx context.Context) error {
	// Add grace period to prevent rapid failover
	gracePeriod := m.gracePeriod
	now := time.Now().Add(-gracePeriod)

	// First check if there's an existing lock and if it's expired
	currentLock, err := m.GetLockInfo(ctx)
	if err != nil && !errors.Is(err, ErrLockNotFound) {
		return fmt.Errorf("failed to check existing lock: %w", err)
	}

	// If lock exists and is not expired, we can't acquire it
	if err == nil && now.Before(currentLock.Expiry) {
		return ErrLockExists
	}

	// Important: Check the last known term and ensure we advance it
	var newTerm int64
	if currentLock == nil {
		newTerm = 1
	} else {
		newTerm = currentLock.Term + 1
	}

	m.term.Store(newTerm)

	// Create new fence token and last known leader
	lastKnownLeader := ""
	newFenceToken := int64(0)
	existingObservers := make(map[string]observerInfo)

	if currentLock != nil {
		newFenceToken = currentLock.FenceToken + 1
		lastKnownLeader = currentLock.Node
		// Preserve existing observers but mark them as needing heartbeat renewal
		for id, observer := range currentLock.Observers {
			observer.IsActive = false // Require new heartbeat
			existingObservers[id] = observer
		}
	}

	// Lock doesn't exist or is expired, try to acquire it
	newVersion := fmt.Sprintf("%d-%s-%d", now.UnixNano(), m.nodeID, newTerm)
	lockInfo := LockInfo{
		Node:            m.nodeID,
		Timestamp:       now,
		Expiry:          now.Add(m.ttl),
		Term:            newTerm,
		Version:         newVersion,
		FenceToken:      newFenceToken,
		LastKnownLeader: lastKnownLeader,
		Observers:       existingObservers,
	}

	lockData, err := json.Marshal(lockInfo)
	if err != nil {
		return fmt.Errorf("failed to marshal lock info: %w", err)
	}

	// Create a new key with our attempt
	attemptKey := fmt.Sprintf("%s.attempt.%s", m.lockKey, lockInfo.Version)

	// First create our attempt atomically
	input := &s3.PutObjectInput{
		Bucket:      aws.String(m.bucket),
		Key:         aws.String(attemptKey),
		Body:        bytes.NewReader(lockData),
		ContentType: aws.String("application/json"),
		IfNoneMatch: aws.String("*"), // Ensure atomic creation
	}

	_, err = m.s3Client.PutObject(ctx, input)
	if err != nil {
		if isAWSErrorCode(err, "PreconditionFailed") {
			// Another node is also trying to acquire the lock
			return ErrLockExists
		}

		return fmt.Errorf("failed to create lock attempt: %w", err)
	}

	// Successfully created our attempt, now verify we're still the most recent attempt
	// and move it to the main lock key
	afterAttempt, err := m.GetLockInfo(ctx)
	if err != nil && !errors.Is(err, ErrLockNotFound) {
		// Clean up our attempt
		_, _ = m.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(m.bucket),
			Key:    aws.String(attemptKey),
		})

		return fmt.Errorf("failed to verify lock state: %w", err)
	}

	// If there's a valid lock now, someone beat us to it
	if err == nil && now.Before(afterAttempt.Expiry) {
		// Clean up our attempt
		_, _ = m.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(m.bucket),
			Key:    aws.String(attemptKey),
		})

		return ErrLockExists
	}

	// Move our attempt to the main lock key
	input = &s3.PutObjectInput{
		Bucket:      aws.String(m.bucket),
		Key:         aws.String(m.lockKey),
		Body:        bytes.NewReader(lockData),
		ContentType: aws.String("application/json"),
	}

	_, err = m.s3Client.PutObject(ctx, input)
	if err != nil {
		// Clean up our attempt
		_, _ = m.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(m.bucket),
			Key:    aws.String(attemptKey),
		})

		return fmt.Errorf("failed to acquire lock: %w", err)
	}

	// Update lease information
	m.lease.UpdateLease(&lockInfo)

	// Clean up our attempt
	_, _ = m.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(attemptKey),
	})

	return nil
}

// renewLock attempts to update the lock using atomic operations.
func (m *Manager) renewLock(ctx context.Context) error {
	// First get current state
	result, err := m.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(m.lockKey),
	})
	if err != nil {
		var noSuchKey *types.NoSuchKey
		if errors.As(err, &noSuchKey) {
			return ErrLockNotFound
		}

		return fmt.Errorf("failed to get current lock: %w", err)
	}
	defer result.Body.Close()

	var currentLock LockInfo
	if err := json.NewDecoder(result.Body).Decode(&currentLock); err != nil {
		return fmt.Errorf("failed to decode lock info: %w", err)
	}

	// Get current lease info
	currentLease := m.lease.GetLeaseInfo()

	// If we have a current lease, verify everything matches
	if currentLease != nil {
		if currentLock.Node != m.nodeID ||
			currentLock.Term != currentLease.Term ||
			currentLock.Version != currentLease.Version {
			return ErrLockModified
		}
	} else {
		// If we don't have a lease but the lock exists and belongs to us,
		// adopt it (this handles the initial renewal case)
		if currentLock.Node == m.nodeID && currentLock.Term == m.getCurrentTerm() {
			m.lease.UpdateLease(&currentLock)
		} else {
			return ErrLockModified
		}
	}

	// Create new lock info with updated timestamp and version
	now := time.Now()
	currentTerm := m.getCurrentTerm()
	newVersion := fmt.Sprintf("%d-%s-%d", now.UnixNano(), m.nodeID, currentTerm)
	newLock := LockInfo{
		Node:      m.nodeID,
		Timestamp: now,
		Expiry:    now.Add(m.ttl),
		Term:      currentTerm,
		Version:   newVersion,
		Observers: currentLock.Observers,
	}

	lockData, err := json.Marshal(newLock)
	if err != nil {
		return fmt.Errorf("failed to marshal lock info: %w", err)
	}

	// Create a new key for the update
	updateKey := fmt.Sprintf("%s.%s", m.lockKey, newVersion)

	// Attempt to create new version
	input := &s3.PutObjectInput{
		Bucket:      aws.String(m.bucket),
		Key:         aws.String(updateKey),
		Body:        bytes.NewReader(lockData),
		ContentType: aws.String("application/json"),
		IfNoneMatch: aws.String("*"), // Ensure atomic update
	}

	_, err = m.s3Client.PutObject(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to create new lock version: %w", err)
	}

	// Move new version to main lock key
	input = &s3.PutObjectInput{
		Bucket:      aws.String(m.bucket),
		Key:         aws.String(m.lockKey),
		Body:        bytes.NewReader(lockData),
		ContentType: aws.String("application/json"),
	}

	_, err = m.s3Client.PutObject(ctx, input)
	if err != nil {
		// Clean up temporary key
		_, _ = m.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(m.bucket),
			Key:    aws.String(updateKey),
		})

		return fmt.Errorf("failed to update main lock: %w", err)
	}

	// Update lease information
	m.lease.UpdateLease(&newLock)

	// Clean up temporary key
	_, _ = m.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(updateKey),
	})

	return nil
}

func (m *Manager) Run(ctx context.Context) error {
	leaderState := &leaderState{
		manager:  m,
		isLeader: false,
	}

	// Initialize self-registration after acquiring lock
	selfRegistered := !m.requireQuorum // if quorum not required, consider self registered

	for {
		select {
		case <-ctx.Done():
			leaderState.handleDemotion(ctx)

			return ctx.Err()

		default:
			// Try to become leader first
			if err := leaderState.runLeaderLoop(ctx); err != nil {
				if !errors.Is(err, context.Canceled) {
					log.Printf("Leader loop error: %v\n", err)
				}

				return err
			}

			// If we're the leader and need quorum but haven't registered, do it now
			if leaderState.isLeader && m.requireQuorum && !selfRegistered {
				if err := m.RegisterObserver(ctx, m.nodeID, nil); err != nil {
					return fmt.Errorf("failed to register self as observer: %w", err)
				}

				selfRegistered = true

				// Start heartbeat goroutine
				heartbeatTicker := time.NewTicker(m.ttl / defaultHeartbeatDivider)
				defer heartbeatTicker.Stop()

				go func() {
					for {
						select {
						case <-ctx.Done():
							return
						case <-heartbeatTicker.C:
							if err := m.UpdateHeartbeat(ctx, m.nodeID); err != nil {
								log.Printf("Failed to update heartbeat: %v", err)
							}
						}
					}
				}()
			}

			time.Sleep(m.pollInterval)
		}
	}
}

// GetLockInfo retrieves current lock information.
func (m *Manager) GetLockInfo(ctx context.Context) (*LockInfo, error) {
	result, err := m.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(m.lockKey),
	})
	if err != nil {
		var noSuchKey *types.NoSuchKey
		if errors.As(err, &noSuchKey) {
			return nil, ErrLockNotFound
		}

		return nil, fmt.Errorf("failed to get lock info: %w", err)
	}
	defer result.Body.Close()

	var lockInfo LockInfo
	if err := json.NewDecoder(result.Body).Decode(&lockInfo); err != nil {
		return nil, fmt.Errorf("failed to decode lock info: %w", err)
	}

	return &lockInfo, nil
}

// RegisterObserver adds a node to the observers list through S3.
func (m *Manager) RegisterObserver(ctx context.Context, nodeID string, metadata map[string]string) error {
	if nodeID == "" {
		return fmt.Errorf("%w: nodeID cannot be empty", ErrInvalidConfig)
	}

	// Retry registration a few times in case of conflicts
	for range 3 {
		// Get current lock info
		lockInfo, err := m.GetLockInfo(ctx)
		if err != nil && !errors.Is(err, ErrLockNotFound) {
			return fmt.Errorf("failed to get lock info before registration: %w", err)
		}

		// If no lock exists yet, we can't register observers
		if lockInfo == nil {
			log.Printf("DEBUG: No lock exists when trying to register observer %s", nodeID)

			return ErrNoActiveLock
		}

		log.Printf("DEBUG: Current lock before registration - Node: %s, Term: %d, Version: %s, Observer count: %d",
			lockInfo.Node, lockInfo.Term, lockInfo.Version, len(lockInfo.Observers))

		// Deep copy the lock info to prevent modification of the original
		newLockInfo := &LockInfo{
			Node:            lockInfo.Node,
			Timestamp:       lockInfo.Timestamp,
			Expiry:          lockInfo.Expiry,
			Term:            lockInfo.Term,
			Version:         lockInfo.Version,
			FenceToken:      lockInfo.FenceToken,
			LastKnownLeader: lockInfo.LastKnownLeader,
		}

		// Initialize or copy observers map
		if lockInfo.Observers == nil {
			newLockInfo.Observers = make(map[string]observerInfo)

			log.Printf("DEBUG: Initializing new observers map for lock")
		} else {
			// Deep copy existing observers
			newLockInfo.Observers = make(map[string]observerInfo, len(lockInfo.Observers))
			for k, v := range lockInfo.Observers {
				newLockInfo.Observers[k] = v
			}
		}

		// Update observer info
		newLockInfo.Observers[nodeID] = observerInfo{
			LastHeartbeat: time.Now(),
			Metadata:      metadata,
			IsActive:      true,
		}

		log.Printf("DEBUG: New lock after adding observer - Node: %s, Term: %d, Version: %s, Observer count: %d",
			newLockInfo.Node, newLockInfo.Term, newLockInfo.Version, len(newLockInfo.Observers))

		// Marshal updated lock info
		lockData, err := json.Marshal(newLockInfo)
		if err != nil {
			return fmt.Errorf("failed to marshal lock info: %w", err)
		}

		// Update S3 with new observer info while preserving lock
		input := &s3.PutObjectInput{
			Bucket:      aws.String(m.bucket),
			Key:         aws.String(m.lockKey),
			Body:        bytes.NewReader(lockData),
			ContentType: aws.String("application/json"),
		}

		_, err = m.s3Client.PutObject(ctx, input)
		if err != nil {
			var apiErr smithy.APIError
			if errors.As(err, &apiErr) && apiErr.ErrorCode() == "PreconditionFailed" {
				log.Printf("DEBUG: Conflict during registration of observer %s, retrying", nodeID)

				continue
			}

			return fmt.Errorf("failed to update lock info: %w", err)
		}

		// Verify the registration
		verifyLock, err := m.GetLockInfo(ctx)
		if err != nil {
			return fmt.Errorf("failed to verify registration: %w", err)
		}

		if verifyLock.Observers == nil || verifyLock.Observers[nodeID].LastHeartbeat.IsZero() {
			log.Printf("DEBUG: Observer %s not found in lock after registration", nodeID)

			continue
		}

		log.Printf("DEBUG: Successfully registered observer %s, total observers: %d",
			nodeID, len(verifyLock.Observers))

		return nil
	}

	return ErrFailedToRegisterObserver
}

// UpdateHeartbeat updates the last heartbeat time for a node in S3.
func (m *Manager) UpdateHeartbeat(ctx context.Context, nodeID string) error {
	for range 3 {
		// Get current lock info
		lockInfo, err := m.GetLockInfo(ctx)
		if err != nil {
			return fmt.Errorf("failed to get lock info: %w", err)
		}

		log.Printf("DEBUG: Updating heartbeat for %s, current lock - Node: %s, Term: %d, Observer count: %d",
			nodeID, lockInfo.Node, lockInfo.Term, len(lockInfo.Observers))

		if lockInfo.Observers == nil {
			log.Printf("DEBUG: No observers map found during heartbeat update for %s", nodeID)

			return ErrNoObserversRegistered
		}

		observer, exists := lockInfo.Observers[nodeID]
		if !exists {
			log.Printf("DEBUG: Node %s not found in observers map", nodeID)

			return fmt.Errorf("%w: node %s", ErrInvalidConfig, nodeID)
		}

		// Update heartbeat while preserving all other data
		observer.LastHeartbeat = time.Now()
		observer.IsActive = true
		lockInfo.Observers[nodeID] = observer

		// Marshal updated lock info
		lockData, err := json.Marshal(lockInfo)
		if err != nil {
			return fmt.Errorf("failed to marshal lock info: %w", err)
		}

		// Update S3 while preserving lock
		input := &s3.PutObjectInput{
			Bucket:      aws.String(m.bucket),
			Key:         aws.String(m.lockKey),
			Body:        bytes.NewReader(lockData),
			ContentType: aws.String("application/json"),
		}

		_, err = m.s3Client.PutObject(ctx, input)
		if err != nil {
			var apiErr smithy.APIError
			if errors.As(err, &apiErr) && apiErr.ErrorCode() == "PreconditionFailed" {
				log.Printf("DEBUG: Conflict during heartbeat update for %s, retrying", nodeID)

				continue
			}

			return fmt.Errorf("failed to update lock info: %w", err)
		}

		log.Printf("DEBUG: Successfully updated heartbeat for %s", nodeID)

		return nil
	}

	return ErrFailedToUpdateHeartbeat
}

// GetActiveObservers returns the count of currently active observers from S3.
func (m *Manager) GetActiveObservers(ctx context.Context) (int, error) {
	lockInfo, err := m.GetLockInfo(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get lock info: %w", err)
	}

	if lockInfo.Observers == nil {
		return 0, nil
	}

	active := 0
	now := time.Now()

	for _, observer := range lockInfo.Observers {
		if observer.IsActive && now.Sub(observer.LastHeartbeat) < m.ttl {
			active++
		}
	}

	return active, nil
}

// Modified the Manager's verifyQuorum method for more aggressive checking.
func (m *Manager) verifyQuorum(ctx context.Context) bool {
	if !m.requireQuorum {
		return true // Always return true if quorum checking is disabled
	}

	lockInfo, err := m.GetLockInfo(ctx)
	if err != nil {
		log.Printf("Failed to get lock info during quorum check: %v", err)

		return false
	}

	now := time.Now()
	activeCount := 0

	// Count active observers that haven't expired
	for nodeID, observer := range lockInfo.Observers {
		if observer.IsActive && now.Sub(observer.LastHeartbeat) < m.ttl {
			activeCount++

			log.Printf("DEBUG: Node %s is active in quorum check, last heartbeat: %v",
				nodeID, now.Sub(observer.LastHeartbeat))
		} else {
			log.Printf("DEBUG: Node %s is inactive in quorum check, last heartbeat: %v",
				nodeID, now.Sub(observer.LastHeartbeat))
		}
	}

	hasQuorum := activeCount >= m.requiredObservers
	log.Printf("DEBUG: Quorum check - Active: %d, Required: %d, Has Quorum: %v",
		activeCount, m.requiredObservers, hasQuorum)

	return hasQuorum
}
