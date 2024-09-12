// Package jobman provides functionality for managing job processing units called ponds.
// It includes error definitions, constants for shared pond operations, and utility functions
// for creating and managing ponds and their associated job queues and worker pools.
//
// It includes the following key concepts:
//
// Job:
//   - Represents a unit of work to be processed by the system.
//   - Implements the Job interface, which defines methods to handle job lifecycle events.
//   - Each job has a unique identifier, a group it belongs to, and an optional partition.
//
// Manager:
//   - The main entry point for submitting jobs to the system.
//   - Manages a collection of groups, each with its own set of ponds.
//   - Responsible for allocating jobs to the appropriate ponds based on the group and partition.
//   - Provides methods to resize the queue and pool of ponds.
//
// Group:
//   - Represents a collection of ponds, including a shared pond and partition ponds.
//   - Manages the context, logger, and counters for received and enqueued items.
//   - Initializes ponds for specific partitions when needed.
//
// Pond:
//   - Represents a job processing unit with a queue and a pool of workers.
//   - Manages the lifecycle, logging, and counters for various job states.
//   - Can be a shared pond or a partition pond.
//   - Shared ponds maintain a list of external queues to dequeue jobs from.
//
// Allocation:
//   - Defines the allocation of a job to a pond of a group, including the size of the queue and pool.
//   - GroupID represents the identifier of the group to which the job is allocated.
//   - PondID represents the identifier of the pond within the group. An empty PondID indicates allocation to a shared pond.
//   - IsShared indicates whether the job is allocated to a shared pond.
//   - QueueSize represents the size of the queue associated with the pond.
//   - PoolSize represents the size of the pool associated with the pond.
//
// AllocatorFunc:
//   - A function type that defines the signature for allocating a job to a pond of a group.
//   - Takes the group and partition as input parameters and returns an Allocation or an error.
//   - If the partition is empty, the job should be allocated to a shared pond, and the size of the queue and pool of the shared pond should be returned.
//
// The package also includes the following strategies for managing ponds:
//
// Shared Pond:
//   - Maintains a queue and a pool of workers.
//   - Dequeues jobs from its own queue and from the external queues of partition ponds.
//   - Has a dedicated watch loop that periodically checks the queue and dequeues jobs.
//   - Configurable parameters for dequeue retry interval and limit.
//
// Partition Pond:
//   - Maintains a queue and a pool of workers for a specific partition.
//   - Dequeues jobs from its own queue and submits them to the worker pool.
//   - Has a dedicated watch loop that periodically checks the queue and dequeues jobs.
//
// The strategy of using shared and partition ponds allows for flexible and efficient job
// processing. Shared ponds provide a centralized processing unit that can handle overflow from
// partition ponds, while partition ponds ensure that jobs for specific partitions are handled
// separately. This approach helps balance the workload across different ponds and ensures that
// resources are utilized effectively.
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
	// ErrInvalidGroupID is an error that indicates that the specified group ID is invalid.
	ErrInvalidGroupID = errors.New("jobman: invalid group id")
	// ErrInvalidPoolSize is an error that indicates that the specified pool size is invalid.
	ErrInvalidPoolSize = errors.New("jobman: invalid pool size")
	// ErrInvalidQueueSize is an error that indicates that the specified queue size is invalid.
	ErrInvalidQueueSize = errors.New("jobman: invalid queue size")
	// ErrExpectedSharedPond is an error that indicates that the expected pond is not a shared pond.
	ErrExpectedSharedPond = errors.New("jobman: expected shared pond")
)

var (
	// SharedPondCheckInterval defines the interval for checking the shared pond.
	SharedPondCheckInterval = 50 * time.Millisecond
	// SharedPondDequeueRetryInterval defines the interval for retrying dequeue operations in the shared pond.
	SharedPondDequeueRetryInterval = 20 * time.Millisecond
	// SharedPondDequeueRetryLimit defines the maximum number of retry attempts for dequeue operations in the shared pond.
	SharedPondDequeueRetryLimit = uint(3)
)

const (
	emojiStat    = "📊"
	emojiAlloc   = "🧬"
	emojiPond    = "🗳️"
	emojiGroup   = "🗂️"
	emojiManager = "📨"
)
