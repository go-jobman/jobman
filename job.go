package jobman

import (
	"errors"
	"time"
)

// Job interface defines the methods that any job should implement.
type Job interface {
	ID() string        // Return the unique identifier for the job.
	Group() string     // Return the group id of the job that the job belongs to.
	Partition() string // Return the partition id of the job (if any), empty string for shared partition.
	OnAccepted()       // Called when the job is submitted successfully to a pond if it's not nil.
	OnRejected()       // Called when the job is rejected by a pond if it's not nil.
	Proceed()          // Called when the job is about to be executed by a worker.
}

// Allocation struct defines the allocation of a job to a pond of a group, and the size of the queue and pool of the pond.
// GroupID represents the identifier of the group to which the job is allocated.
// PondID represents the identifier of the pond within the group. An empty PondID indicates allocation to a shared pond.
// IsShared indicates whether the job is allocated to a shared pond.
// QueueSize represents the size of the queue associated with the pond.
// PoolSize represents the size of the pool associated with the pond.
type Allocation struct {
	GroupID   string `json:"group,omitempty"`
	PondID    string `json:"pond,omitempty"`
	IsShared  bool   `json:"is_shared,omitempty"`
	QueueSize int    `json:"queue_size,omitempty"`
	PoolSize  int    `json:"pool_size,omitempty"`
}

// IsValid checks if the allocation is valid.
func (a Allocation) IsValid() bool {
	return a.GroupID != "" && a.QueueSize > 0 && a.PoolSize > 0
}

// AllocatorFunc is a function type that defines the signature for allocating a job to a pond of a group.
// It takes the group and partition as input parameters and returns an Allocation or an error.
// If the partition is empty, the job should be allocated to a shared pond, and the size of the queue and pool of the shared pond should be returned.
type AllocatorFunc func(group, partition string) (Allocation, error)

// AllocatedJob is actually a wrapper for job with an index in the pond and the lock, used for queueing.
type AllocatedJob struct {
	readyProc chan struct{}
	PondIndex int64
	SubmitAt  time.Time
	Job       Job
}

var (
	// ErrJobNil is an error that indicates that the job is nil.
	ErrJobNil = errors.New("job is nil")
	// ErrPondClosed is an error that indicates that the pond is closed.
	ErrPondClosed = errors.New("pond is closed")
	// ErrGroupNotFound is an error that indicates that the specified group was not found.
	ErrGroupNotFound = errors.New("group not found")
	// ErrPondNotFound is an error that indicates that the specified pond was not found.
	ErrPondNotFound = errors.New("pond not found")
)

var (
	// SharedPondCheckInterval defines the interval for checking the shared pond.
	SharedPondCheckInterval = 100 * time.Millisecond
	// SharedPondDequeueRetryInterval defines the interval for retrying dequeue operations in the shared pond.
	SharedPondDequeueRetryInterval = 30 * time.Millisecond
	// SharedPondDequeueRetryLimit defines the maximum number of retry attempts for dequeue operations in the shared pond.
	SharedPondDequeueRetryLimit = uint(3)
)
