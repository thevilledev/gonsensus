// Package gonsensus provides distributed consensus using S3 conditional operations
package gonsensus

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	ErrLockExists               = errors.New("lock already exists")
	ErrLockNotFound             = errors.New("lock not found")
	ErrLockModified             = errors.New("lock was modified")
	ErrInvalidConfig            = errors.New("invalid configuration")
	ErrLostQuorum               = errors.New("lost quorum")
	ErrNoActiveLock             = errors.New("no active lock exists")
	ErrNoObserversRegistered    = errors.New("no observers registered")
	ErrFailedToUpdateHeartbeat  = errors.New("failed to update heartbeat")
	ErrFailedToRegisterObserver = errors.New("failed to register observer")
	ErrFailedToMarshalLockInfo  = errors.New("failed to marshal lock info")
	ErrFailedToGetLockInfo      = errors.New("failed to get lock info")
	ErrRetryRegistration        = errors.New("retry registration attempt")
)

const (
	defaultTTL                = 30 * time.Second
	defaultPollInterval       = 5 * time.Second
	retryIntervalDivider      = 3  // 1/3 of set TTL
	defaultGracePeriodDivider = 10 // 1/10 of set TTL
	defaultHeartbeatDivider   = 3  // 1/3 of set TTL
	defaultQuorumSize         = 1

	jsonContentType = "application/json"
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
	QuorumSize   int
	GracePeriod  time.Duration
}
