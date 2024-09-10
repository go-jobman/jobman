// Package jobman provides functionality for managing job processing units called ponds.
// It includes error definitions, constants for shared pond operations, and utility functions
// for creating and managing ponds and their associated job queues and worker pools.
//
// Concepts:
//
// Job:
// A job represents a unit of work that needs to be processed. Jobs implement the Job interface,
// which includes methods for getting the job's ID, group, partition, and lifecycle events such as
// OnAccepted, OnRejected, and Proceed.
//
// Manager:
// The Manager is the main entry point for submitting jobs. It manages multiple groups of ponds
// and is responsible for job allocation and dispatching. The manager uses an allocator function
// to determine how jobs are distributed across different ponds within groups.
//
// Group:
// A group represents a collection of ponds, including a shared pond and multiple partition ponds.
// The group manages the context, logger, and counters for received and enqueued items. It ensures
// that jobs are appropriately allocated to either the shared pond or specific partition ponds.
//
// Pond:
// A pond is a job processing unit with a queue and a pool of workers. Ponds manage the lifecycle
// of jobs, including submission, queuing, and execution. Ponds can be either shared or partitioned.
//
// Shared Pond:
// A shared pond is a pond that handles jobs that do not belong to any specific partition. It can
// subscribe to external queues from partition ponds and process jobs from those queues as well.
//
// Partition Pond:
// A partition pond is a pond dedicated to handling jobs belonging to a specific partition. Each
// partition pond has its own queue and worker pool.
//
// Strategy of Shared and Partition Ponds:
//
// Shared Ponds:
// Shared ponds are designed to handle jobs that do not belong to any specific partition. They have
// a central queue and worker pool that can process jobs from multiple sources. Shared ponds also
// subscribe to external queues from partition ponds, allowing them to pick up jobs from those
// queues when their own queue is empty or when additional workers are available.
//
// Partition Ponds:
// Partition ponds handle jobs that belong to specific partitions. Each partition pond has its own
// queue and worker pool, ensuring that jobs for that partition are processed independently. This
// allows for more granular control over job processing and resource allocation for different
// partitions.
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
)

var (
	// SharedPondCheckInterval defines the interval for checking the shared pond.
	SharedPondCheckInterval = 50 * time.Millisecond
	// SharedPondDequeueRetryInterval defines the interval for retrying dequeue operations in the shared pond.
	SharedPondDequeueRetryInterval = 20 * time.Millisecond
	// SharedPondDequeueRetryLimit defines the maximum number of retry attempts for dequeue operations in the shared pond.
	SharedPondDequeueRetryLimit = uint(3)
)
