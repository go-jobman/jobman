package jobman_test

import (
	"errors"
	"testing"

	"gopkg.in/jobman.v0"
)

func TestNewManager(t *testing.T) {
	manager := jobman.NewManager("test-manager")
	if manager == nil {
		t.Error("expected manager to be created, got nil")
	}

	if manager.String() != "📨Manager[test-manager](Groups:0,Received:0)" {
		t.Errorf("unexpected string representation: %s", manager.String())
	}

	if manager.GetName() != "test-manager" {
		t.Errorf("expected manager name: %s, got: %s", "test-manager", manager.GetName())
	}
}

func TestManager_SetAllocator(t *testing.T) {
	manager := jobman.NewManager("test-manager")
	manager.SetAllocator(func(group, partition string) (jobman.Allocation, error) {
		return jobman.Allocation{
			GroupID:   group,
			PondID:    partition,
			IsShared:  partition == "",
			QueueSize: 5,
			PoolSize:  3,
		}, nil
	})
}

func TestManager_ResizeQueue(t *testing.T) {
	manager := jobman.NewManager("test-manager")
	manager.SetAllocator(func(group, partition string) (jobman.Allocation, error) {
		return jobman.Allocation{
			GroupID:   group,
			PondID:    partition,
			IsShared:  partition == "",
			QueueSize: 5,
			PoolSize:  3,
		}, nil
	})

	job := &MockJob{id: "job1", group: "group1"}
	if err := manager.Dispatch(job); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if err := manager.ResizeQueue("group1", "", 10); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if err := manager.ResizeQueue("group1", "missing", 10); err == nil {
		t.Error("expected error, got nil")
	}
}

func TestManager_ResizePool(t *testing.T) {
	manager := jobman.NewManager("test-manager")
	manager.SetAllocator(func(group, partition string) (jobman.Allocation, error) {
		return jobman.Allocation{
			GroupID:   group,
			PondID:    partition,
			IsShared:  partition == "",
			QueueSize: 5,
			PoolSize:  3,
		}, nil
	})

	job := &MockJob{id: "job1", group: "group1"}
	if err := manager.Dispatch(job); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if err := manager.ResizePool("group1", "", 5); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if err := manager.ResizePool("group1", "missing", 5); err == nil {
		t.Error("expected error, got nil")
	}
}

func TestManager_InvalidExtraAllocation(t *testing.T) {
	manager := jobman.NewManager("test-manager")
	manager.SetAllocator(func(group, partition string) (jobman.Allocation, error) {
		return jobman.Allocation{
			GroupID:   group,
			PondID:    partition,
			IsShared:  false,
			QueueSize: 1,
			PoolSize:  3,
		}, nil
	})

	job := &MockJob{id: "job1", group: "group1"}
	if _, err := manager.DispatchWithAllocation(job); err == nil {
		t.Error("expected error, got nil")
	}
}

func TestManager_Dispatch(t *testing.T) {
	manager := jobman.NewManager("test-manager")

	job := &MockJob{id: "job1", group: "group1"}
	if err := manager.Dispatch(job); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Ensure Dispatch calls DispatchWithAllocation correctly
	if al, err := manager.DispatchWithAllocation(job); err != nil {
		t.Errorf("unexpected error: %v", err)
	} else if al == nil {
		t.Error("expected allocation, got nil")
	} else {
		t.Logf("allocation: %v", al)
	}
}

func TestManager_DispatchWithNilJob(t *testing.T) {
	manager := jobman.NewManager("test-manager")
	err := manager.Dispatch(nil)
	if !errors.Is(err, jobman.ErrJobNil) {
		t.Errorf("expected error: %v, got: %v", jobman.ErrJobNil, err)
	}
}

func TestManager_DispatchWithNoAllocator(t *testing.T) {
	manager := jobman.NewManager("test-manager")
	manager.SetAllocator(nil)
	job := &MockJob{id: "job1", group: "group1"}
	err := manager.Dispatch(job)
	if !errors.Is(err, jobman.ErrAllocatorNotSet) {
		t.Errorf("expected error: %v, got: %v", jobman.ErrAllocatorNotSet, err)
	}
}

func TestManager_DispatchWithInvalidAllocation(t *testing.T) {
	manager := jobman.NewManager("test-manager")
	manager.SetAllocator(func(group, partition string) (jobman.Allocation, error) {
		return jobman.Allocation{
			GroupID:   "",
			PondID:    partition,
			IsShared:  partition == "",
			QueueSize: 5,
			PoolSize:  3,
		}, nil
	})
	job := &MockJob{id: "job1", group: "group1"}
	err := manager.Dispatch(job)
	if !errors.Is(err, jobman.ErrInvalidGroupID) {
		t.Errorf("expected error: %v, got: %v", jobman.ErrInvalidGroupID, err)
	}
}

func TestManager_DispatchWithNewGroup(t *testing.T) {
	manager := jobman.NewManager("test-manager")
	manager.SetAllocator(func(group, partition string) (jobman.Allocation, error) {
		return jobman.Allocation{
			GroupID:   group,
			IsShared:  true,
			QueueSize: 5,
			PoolSize:  3,
		}, nil
	})
	job := &MockJob{id: "job1", group: "group1"}
	if err := manager.Dispatch(job); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestManager_DispatchWithExistingGroup(t *testing.T) {
	manager := jobman.NewManager("test-manager")
	manager.SetAllocator(func(group, partition string) (jobman.Allocation, error) {
		return jobman.Allocation{
			GroupID:   group,
			IsShared:  true,
			QueueSize: 5,
			PoolSize:  3,
		}, nil
	})
	job1 := &MockJob{id: "job1", group: "group1"}
	if err := manager.Dispatch(job1); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	job2 := &MockJob{id: "job2", group: "group1"}
	if err := manager.Dispatch(job2); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestManager_DispatchToPartitionPond(t *testing.T) {
	manager := jobman.NewManager("test-manager")
	manager.SetAllocator(func(group, partition string) (jobman.Allocation, error) {
		return jobman.Allocation{
			GroupID:   group,
			PondID:    partition,
			IsShared:  partition == "",
			QueueSize: 5,
			PoolSize:  3,
		}, nil
	})
	job := &MockJob{id: "job1", group: "group1", partition: "partition1"}
	if err := manager.Dispatch(job); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestManager_DispatchToSharedPond(t *testing.T) {
	manager := jobman.NewManager("test-manager")
	manager.SetAllocator(func(group, partition string) (jobman.Allocation, error) {
		return jobman.Allocation{
			GroupID:   group,
			PondID:    "",
			IsShared:  true,
			QueueSize: 10,
			PoolSize:  5,
		}, nil
	})

	job1 := &MockJob{id: "job1", group: "group1", partition: "partition1"}
	job2 := &MockJob{id: "job2", group: "group1", partition: "partition2"}

	_, err := manager.DispatchWithAllocation(job1)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	_, err = manager.DispatchWithAllocation(job2)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	blockForHandling() // Allow some time for the handler to proceed

	sharedPond, err := manager.GetPond("group1", "")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	stat := sharedPond.GetStat()
	if stat.DequeuedCount != 2 {
		t.Errorf("expected dequeued count: 2, got: %d", stat.DequeuedCount)
	}
	if stat.ProceededCount != 2 {
		t.Errorf("expected proceeded count: 2, got: %d", stat.ProceededCount)
	}
	if stat.CompletedCount != 2 {
		t.Errorf("expected completed count: 2, got: %d", stat.CompletedCount)
	}
}

// TestManager_GetPond ensures that GetPond correctly retrieves the pond for a given group and partition.
func TestManager_GetPond(t *testing.T) {
	manager := jobman.NewManager("test-manager")
	manager.SetAllocator(func(group, partition string) (jobman.Allocation, error) {
		return jobman.Allocation{
			GroupID:   group,
			PondID:    partition,
			IsShared:  partition == "",
			QueueSize: 10,
			PoolSize:  5,
		}, nil
	})

	job := &MockJob{id: "job1", group: "group1"}
	if err := manager.Dispatch(job); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Test shared pond
	sharedPond, err := manager.GetPond("group1", "")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if sharedPond == nil {
		t.Error("expected shared pond, got nil")
	}

	// Test partition pond
	if err := manager.Dispatch(&MockJob{id: "job2", group: "group1", partition: "partition1"}); err != nil {
		t.Errorf("expected error: %v, got: %v", nil, err)
	}
	partPond, err := manager.GetPond("group1", "partition1")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if partPond == nil {
		t.Error("expected partition pond, got nil")
	}
}

// TestManager_GetGroup ensures that GetGroup correctly retrieves the group for a given group id.
func TestManager_GetGroup(t *testing.T) {
	manager := jobman.NewManager("test-manager")
	manager.SetAllocator(func(group, partition string) (jobman.Allocation, error) {
		return jobman.Allocation{
			GroupID:   group,
			PondID:    partition,
			IsShared:  partition == "",
			QueueSize: 10,
			PoolSize:  5,
		}, nil
	})

	job := &MockJob{id: "job1", group: "group1"}
	if err := manager.Dispatch(job); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	group, err := manager.GetGroup("group1")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if group == nil {
		t.Error("expected group, got nil")
	}
}

// TestManager_AllocationIssues ensures that allocation issues for Dispatch are handled correctly.
func TestManager_AllocationIssues(t *testing.T) {
	manager := jobman.NewManager("test-manager")

	// Test with nil allocator
	manager.SetAllocator(nil)
	job := &MockJob{id: "job1", group: "group1"}
	err := manager.Dispatch(job)
	if !errors.Is(err, jobman.ErrAllocatorNotSet) {
		t.Errorf("expected error: %v, got: %v", jobman.ErrAllocatorNotSet, err)
	}

	// Test with invalid allocation
	manager.SetAllocator(func(group, partition string) (jobman.Allocation, error) {
		return jobman.Allocation{
			GroupID:   "",
			PondID:    partition,
			IsShared:  partition == "",
			QueueSize: 10,
			PoolSize:  5,
		}, nil
	})
	err = manager.Dispatch(job)
	if !errors.Is(err, jobman.ErrInvalidGroupID) {
		t.Errorf("expected error: %v, got: %v", jobman.ErrInvalidGroupID, err)
	}
}

// TestManager_ErrorsWhilePondIsFull ensures that errors while the pond is full are handled correctly.
func TestManager_ErrorsWhilePondIsFull(t *testing.T) {
	manager := jobman.NewManager("test-manager")
	manager.SetAllocator(func(group, partition string) (jobman.Allocation, error) {
		return jobman.Allocation{
			GroupID:   group,
			PondID:    "",
			IsShared:  true,
			QueueSize: 1, // Small queue size for testing
			PoolSize:  1,
		}, nil
	})

	// should got (1+1+2) = 4 jobs to make it full
	job1 := &MockJob{id: "job1", group: "group1", slow: true}
	job2 := &MockJob{id: "job2", group: "group1", slow: true}
	job3 := &MockJob{id: "job3", group: "group1", slow: true}
	job4 := &MockJob{id: "job4", group: "group1", slow: true}
	job5 := &MockJob{id: "job5", group: "group1", slow: true}

	// first job should be accepted and then proceed
	if err := manager.Dispatch(job1); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// for the rest of the jobs, their status is undetermined
	for _, job := range []*MockJob{job2, job3, job4} {
		blockForHandling()
		err := manager.Dispatch(job)
		t.Logf("submit job %s: %v", job.ID(), err)
	}

	// now the pond is full, even the extra internal variables are full
	blockForHandling()
	if err := manager.Dispatch(job5); err == nil {
		t.Error("expected queue full error, got nil")
	}

	t.Logf("show the manager: %v -- %v", manager, manager.GetStat())
}

// Test WithBlockingCallback option of Manager
func TestManager_WithBlockingCallback(t *testing.T) {
	manager := jobman.NewManager("test-manager", jobman.WithBlockingCallback())
	job := &MockJob{id: "job1", group: "group1"}

	if err := manager.Dispatch(job); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	blockForHandling() // allow some time for the handler to proceed
	if !job.IsAccepted() {
		t.Error("expected job to be accepted")
	}
	if !job.IsProceeded() {
		t.Error("expected job to be proceeded")
	}
}

// Test WithResizeOnDispatch option of Manager
func TestManager_WithResizeOnDispatch(t *testing.T) {
	manager := jobman.NewManager("test-manager", jobman.WithResizeOnDispatch())
	manager.SetAllocator(jobman.MakeSimpleAllocator(10, 5)) // Set initial allocation

	job := &MockJob{id: "job1", group: "group1", slow: true}
	if err := manager.Dispatch(job); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	blockForHandling() // allow some time for the handler to proceed

	// Verify the initial pond size and capacity after first dispatch
	pond, err := manager.GetPond("group1", "")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if pond.GetQueue().Cap() != 10 {
		t.Errorf("expected queue capacity: %d, got: %d", 10, pond.GetQueue().Cap())
	}
	if pond.GetPool().Cap() != 5 {
		t.Errorf("expected pool capacity: %d, got: %d", 5, pond.GetPool().Cap())
	}

	// Check queue and pool sizes
	if pond.GetPool().Free() != 4 { // 5 total - 1 job dispatched
		t.Errorf("expected pool free size: %d, got: %d", 4, pond.GetPool().Free())
	}

	// Resize the allocation and dispatch another job
	manager.SetAllocator(jobman.MakeSimpleAllocator(20, 10))
	job2 := &MockJob{id: "job2", group: "group1", slow: true}
	if err := manager.Dispatch(job2); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	blockForHandling() // allow some time for the handler to proceed
	if !job2.IsAccepted() {
		t.Error("expected job to be accepted")
	}
	if !job2.IsProceeded() {
		t.Error("expected job to be proceeded")
	}

	// Verify that the pond size has been resized
	pond, err = manager.GetPond("group1", "")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if pond.GetQueue().Cap() != 20 {
		t.Errorf("expected queue capacity: %d, got: %d", 20, pond.GetQueue().Cap())
	}
	if pond.GetPool().Cap() != 10 {
		t.Errorf("expected pool capacity: %d, got: %d", 10, pond.GetPool().Cap())
	}

	// Check queue and pool sizes again
	if pond.GetPool().Free() != 8 { // 10 total - 2 jobs dispatched
		t.Errorf("expected pool free size: %d, got: %d", 8, pond.GetPool().Free())
	}
}

// Test Combined options of Manager works without conflict
func TestManager_CombinedOptions(t *testing.T) {
	manager := jobman.NewManager("test-manager", jobman.WithBlockingCallback(), jobman.WithResizeOnDispatch())
	manager.SetAllocator(jobman.MakeSimpleAllocator(10, 5)) // Set initial allocation

	job := &MockJob{id: "job1", group: "group1"}
	if err := manager.Dispatch(job); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	blockForHandling() // allow some time for the handler to proceed

	// Resize the allocation and dispatch another job
	manager.SetAllocator(jobman.MakeSimpleAllocator(20, 10))
	job2 := &MockJob{id: "job2", group: "group1"}
	if err := manager.Dispatch(job2); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	blockForHandling() // allow some time for the handler to proceed
	if !job2.IsAccepted() {
		t.Error("expected job to be accepted")
	}
	if !job2.IsProceeded() {
		t.Error("expected job to be proceeded")
	}

	// Verify that the pond size has been resized
	pond, err := manager.GetPond("group1", "")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if pond.GetQueue().Cap() != 20 {
		t.Errorf("expected queue capacity: %d, got: %d", 20, pond.GetQueue().Cap())
	}
	if pond.GetPool().Cap() != 10 {
		t.Errorf("expected pool capacity: %d, got: %d", 10, pond.GetPool().Cap())
	}
}
