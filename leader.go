package gonsensus

import (
	"context"
	"errors"
	"log"
	"time"
)

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
			if s.manager.requireQuorum {
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
