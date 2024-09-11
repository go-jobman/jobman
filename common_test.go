package jobman_test

import (
	"testing"
	"time"

	"gopkg.in/jobman.v0"
)

func TestErrorConstants(t *testing.T) {
	tests := []struct {
		err      error
		expected string
	}{
		{jobman.ErrAllocatorNotSet, "jobman: allocator not set"},
		{jobman.ErrJobNil, "jobman: job is nil"},
		{jobman.ErrPondClosed, "jobman: pond is closed"},
		{jobman.ErrGroupNotFound, "jobman: group not found"},
		{jobman.ErrPondNotFound, "jobman: pond not found"},
		{jobman.ErrInvalidGroupID, "jobman: invalid group id"},
		{jobman.ErrInvalidPoolSize, "jobman: invalid pool size"},
		{jobman.ErrInvalidQueueSize, "jobman: invalid queue size"},
		{jobman.ErrExpectedSharedPond, "jobman: expected shared pond"},
	}

	for _, tt := range tests {
		t.Run(tt.err.Error(), func(t *testing.T) {
			if tt.err.Error() != tt.expected {
				t.Errorf("expected: %s, got: %s", tt.expected, tt.err.Error())
			}
		})
	}
}

func TestSharedPondConstants(t *testing.T) {
	if jobman.SharedPondCheckInterval != 50*time.Millisecond {
		t.Errorf("expected: %v, got: %v", 50*time.Millisecond, jobman.SharedPondCheckInterval)
	}
	if jobman.SharedPondDequeueRetryInterval != 20*time.Millisecond {
		t.Errorf("expected: %v, got: %v", 20*time.Millisecond, jobman.SharedPondDequeueRetryInterval)
	}
	if jobman.SharedPondDequeueRetryLimit != 3 {
		t.Errorf("expected: %d, got: %d", 3, jobman.SharedPondDequeueRetryLimit)
	}
}

func blockForHandling() {
	time.Sleep(jobman.SharedPondCheckInterval * 2)
}
