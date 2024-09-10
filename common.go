// Package jobman provides functionality for managing job processing units called ponds.
// It includes error definitions, constants for shared pond operations, and utility functions
// for creating and managing ponds and their associated job queues and worker pools.
package jobman

import (
	"errors"
	"time"
)

var (
	// ErrAllocatorNotSet is an error that indicates that the allocator function is not set.
	ErrAllocatorNotSet = errors.New("jobman: allocator not set")
	// ErrJobNil is an error that indicates that the job is nil.
	ErrJobNil = errors.New("jobman: job is nil")
	// ErrPondClosed is an error that indicates that the pond is closed.
	ErrPondClosed = errors.New("jobman: pond is closed")
	// ErrGroupNotFound is an error that indicates that the specified group was not found.
	ErrGroupNotFound = errors.New("jobman: group not found")
	// ErrPondNotFound is an error that indicates that the specified pond was not found.
	ErrPondNotFound = errors.New("jobman: pond not found")
)

var (
	// SharedPondCheckInterval defines the interval for checking the shared pond.
	SharedPondCheckInterval = 50 * time.Millisecond
	// SharedPondDequeueRetryInterval defines the interval for retrying dequeue operations in the shared pond.
	SharedPondDequeueRetryInterval = 20 * time.Millisecond
	// SharedPondDequeueRetryLimit defines the maximum number of retry attempts for dequeue operations in the shared pond.
	SharedPondDequeueRetryLimit = uint(3)
)
