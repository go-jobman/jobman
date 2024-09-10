package jobman_test

import (
	"testing"

	"gopkg.in/jobman.v0"
)

// MockJob is a simple mock implementation of the Job interface for testing.
type MockJob struct {
	id        string
	group     string
	partition string
	accepted  bool
	rejected  bool
	proceeded bool
}

func (mj *MockJob) ID() string        { return mj.id }
func (mj *MockJob) Group() string     { return mj.group }
func (mj *MockJob) Partition() string { return mj.partition }
func (mj *MockJob) OnAccepted()       { mj.accepted = true }
func (mj *MockJob) OnRejected()       { mj.rejected = true }
func (mj *MockJob) Proceed()          { mj.proceeded = true }

// TestAllocationValidation tests the validation of Allocation.
func TestAllocationValidation(t *testing.T) {
	tests := []struct {
		name        string
		allocation  jobman.Allocation
		expectError error
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
			expectError: nil,
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
			expectError: jobman.ErrInvalidGroupID,
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
			expectError: jobman.ErrInvalidQueueSize,
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
			expectError: jobman.ErrInvalidPoolSize,
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
			expectError: jobman.ErrExpectedSharedPond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.allocation.IsValid(true)
			if err != tt.expectError {
				t.Errorf("expected error: %v, got: %v", tt.expectError, err)
			}
		})
	}
}
