package gonsensus

import (
	"sync"
	"sync/atomic"
)

// Lease represents the current leadership lease.
type Lease struct {
	mu      sync.RWMutex
	info    *LockInfo
	version atomic.Value // stores string
}

// NewLease creates a new lease instance.
func NewLease() *Lease {
	l := &Lease{}
	l.version.Store("")

	return l
}

// UpdateLease updates the lease information atomically.
func (l *Lease) UpdateLease(info *LockInfo) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.info = info
	l.version.Store(info.Version)
}

// GetCurrentVersion returns the current lease version.
func (l *Lease) GetCurrentVersion() string {
	s, ok := l.version.Load().(string)
	if !ok {
		panic("forcetypeassert")
	}

	return s
}

// GetLeaseInfo returns the current lease information.
func (l *Lease) GetLeaseInfo() *LockInfo {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.info
}
