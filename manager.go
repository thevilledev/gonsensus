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

	quorumSize int

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
		s3Client:     client,
		bucket:       bucket,
		nodeID:       nodeID,
		lockKey:      lockPrefix + "leader",
		ttl:          cfg.TTL,
		pollInterval: cfg.PollInterval,
		lease:        NewLease(),
		gracePeriod:  cfg.GracePeriod,
		quorumSize:   cfg.QuorumSize,
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
	gracePeriod := m.gracePeriod
	now := time.Now().Add(-gracePeriod)

	// Check existing lock
	currentLock, err := m.checkExistingLock(ctx, now)
	if err != nil {
		return err
	}

	// Prepare new lock info
	lockInfo := m.prepareLockInfo(now, currentLock)

	// Create attempt
	attemptKey := fmt.Sprintf("%s.attempt.%s", m.lockKey, lockInfo.Version)
	if err := m.createLockAttempt(ctx, attemptKey, lockInfo); err != nil {
		return err
	}

	// Verify and acquire
	if err := m.verifyAndAcquireLock(ctx, lockInfo, now); err != nil {
		m.cleanupAttempt(ctx, attemptKey)

		return err
	}

	// Update lease and cleanup
	m.lease.UpdateLease(&lockInfo)
	m.cleanupAttempt(ctx, attemptKey)

	return nil
}

func (m *Manager) checkExistingLock(ctx context.Context, now time.Time) (*LockInfo, error) {
	currentLock, err := m.GetLockInfo(ctx)
	if err != nil && !errors.Is(err, ErrLockNotFound) {
		return nil, fmt.Errorf("failed to check existing lock: %w", err)
	}

	if err == nil && now.Before(currentLock.Expiry) {
		return nil, ErrLockExists
	}

	return currentLock, nil
}

func (m *Manager) prepareLockInfo(now time.Time, currentLock *LockInfo) LockInfo {
	// Set term
	newTerm := int64(1)
	if currentLock != nil {
		newTerm = currentLock.Term + 1
	}

	m.term.Store(newTerm)

	// Prepare observers and leader info
	existingObservers := make(map[string]observerInfo)
	lastKnownLeader := ""
	newFenceToken := int64(0)

	if currentLock != nil {
		newFenceToken = currentLock.FenceToken + 1

		lastKnownLeader = currentLock.Node

		for id, observer := range currentLock.Observers {
			observer.IsActive = false
			existingObservers[id] = observer
		}
	}

	return LockInfo{
		Node:            m.nodeID,
		Timestamp:       now,
		Expiry:          now.Add(m.ttl),
		Term:            newTerm,
		Version:         fmt.Sprintf("%d-%s-%d", now.UnixNano(), m.nodeID, newTerm),
		FenceToken:      newFenceToken,
		LastKnownLeader: lastKnownLeader,
		Observers:       existingObservers,
	}
}

func (m *Manager) createLockAttempt(ctx context.Context, attemptKey string, lockInfo LockInfo) error {
	lockData, err := json.Marshal(lockInfo)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrFailedToMarshalLockInfo, err)
	}

	input := &s3.PutObjectInput{
		Bucket:      aws.String(m.bucket),
		Key:         aws.String(attemptKey),
		Body:        bytes.NewReader(lockData),
		ContentType: aws.String(jsonContentType),
		IfNoneMatch: aws.String("*"),
	}

	_, err = m.s3Client.PutObject(ctx, input)
	if err != nil {
		if isAWSErrorCode(err, "PreconditionFailed") {
			return ErrLockExists
		}

		return fmt.Errorf("failed to create lock attempt: %w", err)
	}

	return nil
}

func (m *Manager) verifyAndAcquireLock(ctx context.Context, lockInfo LockInfo, now time.Time) error {
	afterAttempt, err := m.GetLockInfo(ctx)
	if err != nil && !errors.Is(err, ErrLockNotFound) {
		return fmt.Errorf("failed to verify lock state: %w", err)
	}

	if err == nil && now.Before(afterAttempt.Expiry) {
		return ErrLockExists
	}

	input := &s3.PutObjectInput{
		Bucket:      aws.String(m.bucket),
		Key:         aws.String(m.lockKey),
		Body:        bytes.NewReader(must(json.Marshal(lockInfo))),
		ContentType: aws.String(jsonContentType),
	}

	_, err = m.s3Client.PutObject(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}

	return nil
}

func (m *Manager) cleanupAttempt(ctx context.Context, attemptKey string) {
	_, _ = m.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(attemptKey),
	})
}

// renewLock attempts to update the lock using atomic operations.
func (m *Manager) renewLock(ctx context.Context) error {
	// Get and validate current lock
	currentLock, err := m.getCurrentLockForRenewal(ctx)
	if err != nil {
		return err
	}

	// Verify and update lease
	if err := m.verifyAndUpdateLease(currentLock); err != nil {
		return err
	}

	// Create new lock info
	newLock := m.prepareRenewalLockInfo(currentLock)

	// Attempt renewal
	if err := m.attemptLockRenewal(ctx, newLock); err != nil {
		return err
	}

	return nil
}

func (m *Manager) getCurrentLockForRenewal(ctx context.Context) (*LockInfo, error) {
	result, err := m.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(m.bucket),
		Key:    aws.String(m.lockKey),
	})
	if err != nil {
		var noSuchKey *types.NoSuchKey
		if errors.As(err, &noSuchKey) {
			return nil, ErrLockNotFound
		}

		return nil, fmt.Errorf("failed to get current lock: %w", err)
	}
	defer result.Body.Close()

	var currentLock LockInfo
	if err := json.NewDecoder(result.Body).Decode(&currentLock); err != nil {
		return nil, fmt.Errorf("failed to decode lock info: %w", err)
	}

	return &currentLock, nil
}

func (m *Manager) verifyAndUpdateLease(currentLock *LockInfo) error {
	currentLease := m.lease.GetLeaseInfo()

	if currentLease != nil {
		if currentLock.Node != m.nodeID ||
			currentLock.Term != currentLease.Term ||
			currentLock.Version != currentLease.Version {
			return ErrLockModified
		}

		return nil
	}

	// Handle initial renewal case
	if currentLock.Node == m.nodeID && currentLock.Term == m.getCurrentTerm() {
		m.lease.UpdateLease(currentLock)

		return nil
	}

	return ErrLockModified
}

func (m *Manager) prepareRenewalLockInfo(currentLock *LockInfo) LockInfo {
	now := time.Now()
	currentTerm := m.getCurrentTerm()

	return LockInfo{
		Node:      m.nodeID,
		Timestamp: now,
		Expiry:    now.Add(m.ttl),
		Term:      currentTerm,
		Version:   fmt.Sprintf("%d-%s-%d", now.UnixNano(), m.nodeID, currentTerm),
		Observers: currentLock.Observers,
		// Preserve other fields from current lock
		FenceToken:      currentLock.FenceToken,
		LastKnownLeader: currentLock.LastKnownLeader,
	}
}

func (m *Manager) attemptLockRenewal(ctx context.Context, newLock LockInfo) error {
	lockData, err := json.Marshal(newLock)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrFailedToMarshalLockInfo, err)
	}

	// Create temporary update key
	updateKey := fmt.Sprintf("%s.%s", m.lockKey, newLock.Version)

	// Create new version atomically
	if err := m.createLockAttempt(ctx, updateKey, newLock); err != nil {
		return fmt.Errorf("failed to create new lock version: %w", err)
	}

	// Move to main lock key
	if err := m.finalizeRenewal(ctx, lockData); err != nil {
		m.cleanupAttempt(ctx, updateKey)

		return err
	}

	// Update lease and cleanup
	m.lease.UpdateLease(&newLock)
	m.cleanupAttempt(ctx, updateKey)

	return nil
}

func (m *Manager) finalizeRenewal(ctx context.Context, lockData []byte) error {
	input := &s3.PutObjectInput{
		Bucket:      aws.String(m.bucket),
		Key:         aws.String(m.lockKey),
		Body:        bytes.NewReader(lockData),
		ContentType: aws.String(jsonContentType),
	}

	_, err := m.s3Client.PutObject(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to update main lock: %w", err)
	}

	return nil
}

func (m *Manager) Run(ctx context.Context) error {
	leaderState := &leaderState{
		manager:  m,
		isLeader: false,
	}

	// Initialize self-registration after acquiring lock
	selfRegistered := m.quorumSize <= 1 // if quorum not required, consider self registered

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
			if leaderState.isLeader && !selfRegistered {
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

		return nil, fmt.Errorf("%w: %w", ErrFailedToGetLockInfo, err)
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
		if err := m.attemptObserverRegistration(ctx, nodeID, metadata); err != nil {
			if errors.Is(err, ErrRetryRegistration) {
				continue
			}

			return err
		}

		return nil
	}

	return ErrFailedToRegisterObserver
}

func (m *Manager) attemptObserverRegistration(ctx context.Context, nodeID string, metadata map[string]string) error {
	// Get and validate current lock
	lockInfo, err := m.getLockForRegistration(ctx, nodeID)
	if err != nil {
		return err
	}

	// Prepare new lock info with observer
	newLockInfo := m.prepareObserverLockInfo(lockInfo, nodeID, metadata)

	// Update lock in S3
	if err := m.updateLockWithObserver(ctx, newLockInfo, nodeID); err != nil {
		return err
	}

	// Verify registration
	if err := m.verifyObserverRegistration(ctx, nodeID); err != nil {
		return ErrRetryRegistration
	}

	return nil
}

func (m *Manager) getLockForRegistration(ctx context.Context, nodeID string) (*LockInfo, error) {
	lockInfo, err := m.GetLockInfo(ctx)
	if err != nil && !errors.Is(err, ErrLockNotFound) {
		return nil, fmt.Errorf("failed to get lock info before registration: %w", err)
	}

	if lockInfo == nil {
		log.Printf("DEBUG: No lock exists when trying to register observer %s", nodeID)

		return nil, ErrNoActiveLock
	}

	log.Printf("DEBUG: Current lock before registration - Node: %s, Term: %d, Version: %s, Observer count: %d",
		lockInfo.Node, lockInfo.Term, lockInfo.Version, len(lockInfo.Observers))

	return lockInfo, nil
}

func (m *Manager) prepareObserverLockInfo(currentLock *LockInfo, nodeID string, metadata map[string]string) *LockInfo {
	// Deep copy the lock info
	newLockInfo := &LockInfo{
		Node:            currentLock.Node,
		Timestamp:       currentLock.Timestamp,
		Expiry:          currentLock.Expiry,
		Term:            currentLock.Term,
		Version:         currentLock.Version,
		FenceToken:      currentLock.FenceToken,
		LastKnownLeader: currentLock.LastKnownLeader,
	}

	// Initialize or copy observers map
	newLockInfo.Observers = m.initializeObserversMap(currentLock.Observers)

	// Add new observer
	newLockInfo.Observers[nodeID] = observerInfo{
		LastHeartbeat: time.Now(),
		Metadata:      metadata,
		IsActive:      true,
	}

	log.Printf("DEBUG: New lock after adding observer - Node: %s, Term: %d, Version: %s, Observer count: %d",
		newLockInfo.Node, newLockInfo.Term, newLockInfo.Version, len(newLockInfo.Observers))

	return newLockInfo
}

func (m *Manager) initializeObserversMap(existing map[string]observerInfo) map[string]observerInfo {
	if existing == nil {
		log.Printf("DEBUG: Initializing new observers map for lock")

		return make(map[string]observerInfo)
	}

	// Deep copy existing observers
	newMap := make(map[string]observerInfo, len(existing))
	for k, v := range existing {
		newMap[k] = v
	}

	return newMap
}

func (m *Manager) updateLockWithObserver(ctx context.Context, lockInfo *LockInfo, nodeID string) error {
	lockData, err := json.Marshal(lockInfo)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrFailedToMarshalLockInfo, err)
	}

	input := &s3.PutObjectInput{
		Bucket:      aws.String(m.bucket),
		Key:         aws.String(m.lockKey),
		Body:        bytes.NewReader(lockData),
		ContentType: aws.String(jsonContentType),
	}

	_, err = m.s3Client.PutObject(ctx, input)
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "PreconditionFailed" {
			log.Printf("DEBUG: Conflict during registration of observer %s, retrying", nodeID)

			return ErrRetryRegistration
		}

		return fmt.Errorf("failed to update lock info: %w", err)
	}

	return nil
}

func (m *Manager) verifyObserverRegistration(ctx context.Context, nodeID string) error {
	verifyLock, err := m.GetLockInfo(ctx)
	if err != nil {
		return fmt.Errorf("failed to verify registration: %w", err)
	}

	if verifyLock.Observers == nil || verifyLock.Observers[nodeID].LastHeartbeat.IsZero() {
		log.Printf("DEBUG: Observer %s not found in lock after registration", nodeID)

		return ErrRetryRegistration
	}

	log.Printf("DEBUG: Successfully registered observer %s, total observers: %d",
		nodeID, len(verifyLock.Observers))

	return nil
}

// UpdateHeartbeat updates the last heartbeat time for a node in S3.
func (m *Manager) UpdateHeartbeat(ctx context.Context, nodeID string) error {
	for range 3 {
		// Get current lock info
		lockInfo, err := m.GetLockInfo(ctx)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrFailedToGetLockInfo, err)
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
			return fmt.Errorf("%w: %w", ErrFailedToMarshalLockInfo, err)
		}

		// Update S3 while preserving lock
		input := &s3.PutObjectInput{
			Bucket:      aws.String(m.bucket),
			Key:         aws.String(m.lockKey),
			Body:        bytes.NewReader(lockData),
			ContentType: aws.String(jsonContentType),
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
		return 0, fmt.Errorf("%w: %w", ErrFailedToGetLockInfo, err)
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
	if m.quorumSize <= 1 {
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

	hasQuorum := activeCount >= m.quorumSize
	log.Printf("DEBUG: Quorum check - Active: %d, Required: %d, Has Quorum: %v",
		activeCount, m.quorumSize, hasQuorum)

	return hasQuorum
}
