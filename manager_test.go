package jobman_test

import (
	"errors"
	"testing"

	"gopkg.in/jobman.v0"
)

func TestNewManager(t *testing.T) {
	manager := jobman.NewManager("test-manager")
	if manager == nil {
		t.Fatal("expected manager to be created, got nil")
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
		t.Fatalf("unexpected error: %v", err)
	}

	if err := manager.ResizeQueue("group1", "", 10); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := manager.ResizeQueue("group1", "missing", 10); err == nil {
		t.Fatal("expected error, got nil")
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
		t.Fatalf("unexpected error: %v", err)
	}

	if err := manager.ResizePool("group1", "", 5); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := manager.ResizePool("group1", "missing", 5); err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestManager_Dispatch(t *testing.T) {
	manager := jobman.NewManager("test-manager")

	job := &MockJob{id: "job1", group: "group1"}
	if err := manager.Dispatch(job); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Ensure Dispatch calls DispatchWithAllocation correctly
	if _, err := manager.DispatchWithAllocation(job); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestManager_DispatchWithNilJob(t *testing.T) {
	manager := jobman.NewManager("test-manager")
	err := manager.Dispatch(nil)
	if !errors.Is(err, jobman.ErrJobNil) {
		t.Fatalf("expected error: %v, got: %v", jobman.ErrJobNil, err)
	}
}

func TestManager_DispatchWithNoAllocator(t *testing.T) {
	manager := jobman.NewManager("test-manager")
	manager.SetAllocator(nil)
	job := &MockJob{id: "job1", group: "group1"}
	err := manager.Dispatch(job)
	if !errors.Is(err, jobman.ErrAllocatorNotSet) {
		t.Fatalf("expected error: %v, got: %v", jobman.ErrAllocatorNotSet, err)
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
		t.Fatalf("expected error: %v, got: %v", jobman.ErrInvalidGroupID, err)
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
		t.Fatalf("unexpected error: %v", err)
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
		t.Fatalf("unexpected error: %v", err)
	}

	job2 := &MockJob{id: "job2", group: "group1"}
	if err := manager.Dispatch(job2); err != nil {
		t.Fatalf("unexpected error: %v", err)
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
		t.Fatalf("unexpected error: %v", err)
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
		t.Fatalf("unexpected error: %v", err)
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
		t.Fatalf("unexpected error: %v", err)
	}

	// Test shared pond
	sharedPond, err := manager.GetPond("group1", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sharedPond == nil {
		t.Fatal("expected shared pond, got nil")
	}

	// Test partition pond
	if err := manager.Dispatch(&MockJob{id: "job2", group: "group1", partition: "partition1"}); err != nil {
		t.Fatalf("expected error: %v, got: %v", nil, err)
	}
	partPond, err := manager.GetPond("group1", "partition1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if partPond == nil {
		t.Fatal("expected partition pond, got nil")
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
		t.Fatalf("unexpected error: %v", err)
	}

	group, err := manager.GetGroup("group1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if group == nil {
		t.Fatal("expected group, got nil")
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
		t.Fatalf("expected error: %v, got: %v", jobman.ErrAllocatorNotSet, err)
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
		t.Fatalf("expected error: %v, got: %v", jobman.ErrInvalidGroupID, err)
	}
}

// TestManager_ErrorsWhilePondIsFull ensures that errors while the pond is full are handled correctly.
func TestManager_ErrorsWhilePondIsFull(t *testing.T) {
	manager := jobman.NewManager("test-manager")
	manager.SetAllocator(func(group, partition string) (jobman.Allocation, error) {
		return jobman.Allocation{
			GroupID:   group,
			PondID:    partition,
			IsShared:  partition == "",
			QueueSize: 1, // Small queue size for testing
			PoolSize:  1,
		}, nil
	})

	job1 := &MockJob{id: "job1", group: "group1"}
	job2 := &MockJob{id: "job2", group: "group1"}
	// should got (1+1+2) = 4 jobs to make it full

	if err := manager.Dispatch(job1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := manager.Dispatch(job2); err == nil {
		t.Fatal("expected queue full error, got nil")
	} else {
		t.Logf("expected error: %v -- %v", err, manager)
	}
}
