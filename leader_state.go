package gonsensus

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"
)

// leaderState is an internal state machine for leader election.
type leaderState struct {
	manager  *Manager
	isLeader bool
	mu       sync.RWMutex // Add mutex for state protection
}

func (s *leaderState) runLeaderLoop(ctx context.Context) error {
	if !s.getIsLeader() {
		if err := s.tryBecomeLeader(ctx); err != nil {
			return err
		}
	}

	if s.getIsLeader() {
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

	s.setLeader(true)

	return s.handleElection(ctx)
}

func (s *leaderState) handleElection(ctx context.Context) error {
	// Get callback reference under read lock
	s.mu.RLock()
	callback := s.manager.onElected
	s.mu.RUnlock()

	if callback == nil {
		return nil
	}

	// Release lock during potentially long-running callback
	if err := callback(ctx); err != nil {
		log.Printf("Error in leader callback: %v\n", err)
		s.setLeader(false)
	}

	return nil
}

func (s *leaderState) handleDemotion(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Need atomic operation because we need to check state and potentially
	// call callback within the same lock
	if s.isLeader && s.manager.onDemoted != nil {
		s.manager.onDemoted(ctx)
	}

	s.isLeader = false
}

func (s *leaderState) runLeaderMaintenance(ctx context.Context) error {
	ticker := time.NewTicker(s.manager.ttl / retryIntervalDivider)
	defer ticker.Stop()

	for s.getIsLeader() {
		select {
		case <-ctx.Done():
			s.handleDemotion(ctx)

			return ctx.Err()

		case <-ticker.C:
			if s.manager.quorumSize > 1 {
				if !s.manager.verifyQuorum(ctx) {
					log.Printf("Lost quorum, stepping down")
					s.handleDemotion(ctx)

					return ErrLostQuorum
				}
			}

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

func (s *leaderState) setLeader(isLeader bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.isLeader = isLeader
}

func (s *leaderState) getIsLeader() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.isLeader
}
