package gonsensus

import (
	"testing"
	"time"
)

func TestNewLease(t *testing.T) {
	t.Parallel()

	lease := NewLease()
	if lease == nil {
		t.Fatal("NewLease() returned nil")
	}

	// Check initial version is empty string
	if v := lease.GetCurrentVersion(); v != "" {
		t.Errorf("initial version = %q, want empty string", v)
	}

	// Check initial info is nil
	if info := lease.GetLeaseInfo(); info != nil {
		t.Errorf("initial info = %+v, want nil", info)
	}
}

func TestLease_UpdateLease(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		info     *LockInfo
		wantInfo *LockInfo
	}{
		{
			name: "update with valid info",
			info: &LockInfo{
				Node:      "node1",
				Version:   "v1",
				Timestamp: time.Now(),
				Term:      1,
			},
			wantInfo: &LockInfo{
				Node:      "node1",
				Version:   "v1",
				Timestamp: time.Now(),
				Term:      1,
			},
		},
		{
			name: "update with nil observers",
			info: &LockInfo{
				Node:      "node2",
				Version:   "v2",
				Timestamp: time.Now(),
				Term:      2,
				Observers: nil,
			},
			wantInfo: &LockInfo{
				Node:      "node2",
				Version:   "v2",
				Timestamp: time.Now(),
				Term:      2,
				Observers: nil,
			},
		},
		{
			name: "update with observers",
			info: &LockInfo{
				Node:      "node3",
				Version:   "v3",
				Timestamp: time.Now(),
				Term:      3,
				Observers: map[string]ObserverInfo{
					"observer1": {
						LastHeartbeat: time.Now(),
						IsActive:      true,
					},
				},
			},
			wantInfo: &LockInfo{
				Node:      "node3",
				Version:   "v3",
				Timestamp: time.Now(),
				Term:      3,
				Observers: map[string]ObserverInfo{
					"observer1": {
						LastHeartbeat: time.Now(),
						IsActive:      true,
					},
				},
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()

			lease := NewLease()
			lease.UpdateLease(testCase.info)

			// Check version was updated
			if got := lease.GetCurrentVersion(); got != testCase.info.Version {
				t.Errorf("GetCurrentVersion() = %v, want %v", got, testCase.info.Version)
			}

			// Check info was updated
			got := lease.GetLeaseInfo()
			if got == nil {
				t.Fatal("GetLeaseInfo() returned nil")
			}

			// Compare relevant fields
			if got.Node != testCase.wantInfo.Node {
				t.Errorf("Node = %v, want %v", got.Node, testCase.wantInfo.Node)
			}

			if got.Version != testCase.wantInfo.Version {
				t.Errorf("Version = %v, want %v", got.Version, testCase.wantInfo.Version)
			}

			if got.Term != testCase.wantInfo.Term {
				t.Errorf("Term = %v, want %v", got.Term, testCase.wantInfo.Term)
			}

			// Compare observers if present
			if testCase.wantInfo.Observers != nil {
				if len(got.Observers) != len(testCase.wantInfo.Observers) {
					t.Errorf("Observers count = %v, want %v", len(got.Observers), len(testCase.wantInfo.Observers))
				}

				for k, v := range testCase.wantInfo.Observers {
					if gotObs, exists := got.Observers[k]; !exists {
						t.Errorf("Observer %v not found", k)
					} else if gotObs.IsActive != v.IsActive {
						t.Errorf("Observer %v IsActive = %v, want %v", k, gotObs.IsActive, v.IsActive)
					}
				}
			}
		})
	}
}

func TestLease_Concurrent(t *testing.T) {
	t.Parallel()

	lease := NewLease()
	done := make(chan bool)
	iterations := 1000

	// Concurrent writers
	go func() {
		for range iterations {
			lease.UpdateLease(&LockInfo{
				Version: "v1",
				Node:    "node1",
			})
		}
		done <- true
	}()

	// Concurrent readers
	go func() {
		for range iterations {
			_ = lease.GetLeaseInfo()
			_ = lease.GetCurrentVersion()
		}
		done <- true
	}()

	// Wait for both goroutines to finish
	<-done
	<-done
}

func TestLease_GetCurrentVersion_PanicOnInvalidStore(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r == nil {
			t.Error("GetCurrentVersion() should panic on invalid type assertion")
		}
	}()

	lease := NewLease()
	// Force an invalid value into the atomic.Value
	lease.version.Store(123) // Store non-string value

	lease.GetCurrentVersion()
}
