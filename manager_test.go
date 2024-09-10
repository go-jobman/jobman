package jobman_test

import (
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
}

func TestManager_Dispatch(t *testing.T) {
	manager := jobman.NewManager("test-manager")
	job := &MockJob{id: "job1", group: "group1"}

	if err := manager.Dispatch(job); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
