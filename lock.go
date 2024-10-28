package gonsensus

import (
	"time"
)

type LockInfo struct {
	Node            string                  `json:"node"`
	Timestamp       time.Time               `json:"timestamp"`
	Expiry          time.Time               `json:"expiry"`
	Term            int64                   `json:"term"`
	Version         string                  `json:"version"`
	FenceToken      int64                   `json:"fenceToken"`
	LastKnownLeader string                  `json:"lastKnownLeader"`
	Observers       map[string]observerInfo `json:"observers"`
}

// IsExpired checks if a lock is expired.
func (l *LockInfo) IsExpired() bool {
	return time.Now().After(l.Expiry)
}

// IsValid checks if a lock is valid (exists and not expired).
func (l *LockInfo) IsValid() bool {
	return l != nil && !l.IsExpired()
}
