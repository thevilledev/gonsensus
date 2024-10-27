// Package gonsensus provides distributed consensus using S3 conditional operations
package gonsensus

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

var (
	ErrLockExists    = errors.New("lock already exists")
	ErrLockNotFound  = errors.New("lock not found")
	ErrLockModified  = errors.New("lock was modified")
	ErrInvalidConfig = errors.New("invalid configuration")
)

const (
	defaultTTL           = 30 * time.Second
	defaultPollInterval  = 5 * time.Second
	retryIntervalDivider = 3 // always 1/3 of set TTL
)

// S3Client defines the interface for S3 operations.
type S3Client interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput,
		optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput,
		optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput,
		optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

type Config struct {
	TTL          time.Duration
	PollInterval time.Duration
	LockPrefix   string
	NodeID       string
}

type Manager struct {
	s3Client     S3Client
	bucket       string
	nodeID       string
	lockKey      string
	ttl          time.Duration
	term         int64
	pollInterval time.Duration
	onElected    func(context.Context) error
	onDemoted    func(context.Context)
}

// Helper function to check AWS error codes.
func isAWSErrorCode(err error, code string) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode() == code
	}

	return false
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

	return &Manager{
		s3Client:     client,
		bucket:       bucket,
		nodeID:       nodeID,
		lockKey:      lockPrefix + "leader",
		ttl:          cfg.TTL,
		pollInterval: cfg.PollInterval,
	}, nil
}

func (m *Manager) SetCallbacks(onElected func(context.Context) error, onDemoted func(context.Context)) {
	m.onElected = onElected
	m.onDemoted = onDemoted
}

// Check if lock is expired and try to acquire if it is.
func (m *Manager) acquireLock(ctx context.Context) error {
	now := time.Now()

	// First check if there's an existing lock and if it's expired
	currentLock, err := m.GetLockInfo(ctx)
	if err != nil && !errors.Is(err, ErrLockNotFound) {
		return fmt.Errorf("failed to check existing lock: %w", err)
	}

	// If lock exists and is not expired, we can't acquire it
	if err == nil && now.Before(currentLock.Expiry) {
		return ErrLockExists
	}

	// Lock doesn't exist or is expired, try to acquire it
	m.term++ // Increment term for new leadership attempt

	lockInfo := LockInfo{
		Node:      m.nodeID,
		Timestamp: now,
		Expiry:    now.Add(m.ttl),
		Term:      m.term,
		Version:   fmt.Sprintf("%d-%s-%d", now.UnixNano(), m.nodeID, m.term),
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

	// Verify we still own the lock
	if currentLock.Node != m.nodeID || currentLock.Term != m.term {
		return ErrLockModified
	}

	// Create new lock info with updated timestamp and version
	now := time.Now()
	newLock := LockInfo{
		Node:      m.nodeID,
		Timestamp: now,
		Expiry:    now.Add(m.ttl),
		Term:      m.term,
		Version:   fmt.Sprintf("%d-%s-%d", now.UnixNano(), m.nodeID, m.term),
	}

	lockData, err := json.Marshal(newLock)
	if err != nil {
		return fmt.Errorf("failed to marshal lock info: %w", err)
	}

	// Create a new key for the update
	updateKey := fmt.Sprintf("%s.%s", m.lockKey, newLock.Version)

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

	for {
		select {
		case <-ctx.Done():
			leaderState.handleDemotion(ctx)
			return ctx.Err()
		default:
			if err := leaderState.runLeaderLoop(ctx); err != nil {
				if !errors.Is(err, context.Canceled) {
					log.Printf("Leader loop error: %v\n", err)
				}
				return err
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

type LockInfo struct {
	Node      string    `json:"node"`
	Timestamp time.Time `json:"timestamp"`
	Expiry    time.Time `json:"expiry"`
	Term      int64     `json:"term"`
	Version   string    `json:"version"`
}

// IsExpired checks if a lock is expired.
func (l *LockInfo) IsExpired() bool {
	return time.Now().After(l.Expiry)
}

// IsValid checks if a lock is valid (exists and not expired).
func (l *LockInfo) IsValid() bool {
	return l != nil && !l.IsExpired()
}

// leaderState is an internal state machine for leader election.
type leaderState struct {
	manager  *Manager
	isLeader bool
}

func (s *leaderState) runLeaderLoop(ctx context.Context) error {
	if !s.isLeader {
		if err := s.tryBecomeLeader(ctx); err != nil {
			return err
		}
	}

	if s.isLeader {
		return s.runLeaderMaintenance(ctx)
	}

	return nil
}

func (s *leaderState) tryBecomeLeader(ctx context.Context) error {
	err := s.manager.acquireLock(ctx)
	if err != nil {
		if !errors.Is(err, ErrLockExists) {
			log.Printf("Error acquiring lock: %v\n", err)
		}
		return nil
	}

	s.isLeader = true
	return s.handleElection(ctx)
}

func (s *leaderState) handleElection(ctx context.Context) error {
	if s.manager.onElected == nil {
		return nil
	}

	if err := s.manager.onElected(ctx); err != nil {
		log.Printf("Error in leader callback: %v\n", err)
		s.isLeader = false
	}
	return nil
}

func (s *leaderState) handleDemotion(ctx context.Context) {
	if s.isLeader && s.manager.onDemoted != nil {
		s.manager.onDemoted(ctx)
	}
	s.isLeader = false
}

func (s *leaderState) runLeaderMaintenance(ctx context.Context) error {
	ticker := time.NewTicker(s.manager.ttl / retryIntervalDivider)
	defer ticker.Stop()

	for s.isLeader {
		select {
		case <-ctx.Done():
			s.handleDemotion(ctx)
			return ctx.Err()

		case <-ticker.C:
			if err := s.renewLeadership(ctx); err != nil {
				return nil
			}
		}
	}
	return nil
}

func (s *leaderState) renewLeadership(ctx context.Context) error {
	if err := s.manager.renewLock(ctx); err != nil {
		log.Printf("Failed to renew lock: %v\n", err)
		s.handleDemotion(ctx)
		return err
	}
	return nil
}
