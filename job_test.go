package jobman_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"gopkg.in/jobman.v0"
)

func TestAllocation_IsValid(t *testing.T) {
	tests := []struct {
		name         string
		allocation   jobman.Allocation
		expectError  error
		expectShared bool
	}{
		{
			name: "valid allocation",
			allocation: jobman.Allocation{
				GroupID:   "group1",
				PondID:    "",
				IsShared:  true,
				QueueSize: 10,
				PoolSize:  5,
			},
			expectError:  nil,
			expectShared: true,
		},
		{
			name: "invalid group ID",
			allocation: jobman.Allocation{
				GroupID:   "",
				PondID:    "pond1",
				IsShared:  false,
				QueueSize: 10,
				PoolSize:  5,
			},
			expectError:  jobman.ErrInvalidGroupID,
			expectShared: false,
		},
		{
			name: "invalid queue size",
			allocation: jobman.Allocation{
				GroupID:   "group1",
				PondID:    "pond1",
				IsShared:  false,
				QueueSize: 0,
				PoolSize:  5,
			},
			expectError:  jobman.ErrInvalidQueueSize,
			expectShared: false,
		},
		{
			name: "invalid pool size",
			allocation: jobman.Allocation{
				GroupID:   "group1",
				PondID:    "pond1",
				IsShared:  false,
				QueueSize: 10,
				PoolSize:  0,
			},
			expectError:  jobman.ErrInvalidPoolSize,
			expectShared: false,
		},
		{
			name: "expected shared pond",
			allocation: jobman.Allocation{
				GroupID:   "group1",
				PondID:    "pond1",
				IsShared:  false,
				QueueSize: 10,
				PoolSize:  5,
			},
			expectError:  jobman.ErrExpectedSharedPond,
			expectShared: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.allocation.IsValid(tt.expectShared)
			if !errors.Is(err, tt.expectError) {
				t.Errorf("expected error: %v, got: %v", tt.expectError, err)
			}
		})
	}
}

// Test MakeSimpleAllocator function
func TestMakeSimpleAllocator(t *testing.T) {
	allocator := jobman.MakeSimpleAllocator(10, 5)

	allocation, err := allocator("group1", "")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := allocation.IsValid(true); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if allocation.GroupID != "group1" {
		t.Errorf("expected group ID: %s, got: %s", "group1", allocation.GroupID)
	}
	if allocation.QueueSize != 10 {
		t.Errorf("expected queue size: %d, got: %d", 10, allocation.QueueSize)
	}
	if allocation.PoolSize != 5 {
		t.Errorf("expected pool size: %d, got: %d", 5, allocation.PoolSize)
	}
	if !allocation.IsShared {
		t.Errorf("expected IsShared: true, got: %v", allocation.IsShared)
	}

	allocation, err = allocator("group1", "partition1")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := allocation.IsValid(false); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if allocation.GroupID != "group1" {
		t.Errorf("expected group ID: %s, got: %s", "group1", allocation.GroupID)
	}
	if allocation.QueueSize != 10 {
		t.Errorf("expected queue size: %d, got: %d", 10, allocation.QueueSize)
	}
	if allocation.PoolSize != 5 {
		t.Errorf("expected pool size: %d, got: %d", 5, allocation.PoolSize)
	}
	if allocation.IsShared {
		t.Errorf("expected IsShared: false, got: %v", allocation.IsShared)
	}
}

// MockJob is a simple mock implementation of the Job interface for testing.
type MockJob struct {
	mu        sync.Mutex
	id        string
	group     string
	partition string
	accepted  bool
	rejected  bool
	proceeded bool
	slow      bool
}

func (mj *MockJob) ID() string {
	return mj.id
}

func (mj *MockJob) Group() string {
	return mj.group
}

func (mj *MockJob) Partition() string {
	return mj.partition
}

func (mj *MockJob) OnAccepted() {
	mj.mu.Lock()
	defer mj.mu.Unlock()
	mj.accepted = true
}

func (mj *MockJob) OnRejected() {
	mj.mu.Lock()
	defer mj.mu.Unlock()
	mj.rejected = true
}

func (mj *MockJob) Proceed() {
	mj.mu.Lock()
	defer mj.mu.Unlock()
	mj.proceeded = true
	if mj.slow {
		<-time.After(jobman.SharedPondCheckInterval * 20)
	}
}

// Helper methods to check the state of the job in tests
func (mj *MockJob) IsAccepted() bool {
	mj.mu.Lock()
	defer mj.mu.Unlock()
	return mj.accepted
}

func (mj *MockJob) IsRejected() bool {
	mj.mu.Lock()
	defer mj.mu.Unlock()
	return mj.rejected
}

func (mj *MockJob) IsProceeded() bool {
	mj.mu.Lock()
	defer mj.mu.Unlock()
	return mj.proceeded
}
