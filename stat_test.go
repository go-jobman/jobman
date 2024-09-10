package jobman_test

import (
	"testing"

	"gopkg.in/jobman.v0"
)

func TestGroup_GetStat(t *testing.T) {
	group := jobman.NewGroup("test-group", 10, 5)
	group.InitializePond("partition-1", 5, 3)

	stat := group.GetStat()
	if stat.ReceivedCount != 0 {
		t.Errorf("expected received count: %d, got: %d", 0, stat.ReceivedCount)
	}
	if stat.EnqueuedCount != 0 {
		t.Errorf("expected enqueued count: %d, got: %d", 0, stat.EnqueuedCount)
	}
	if stat.PondCapacity != 2 {
		t.Errorf("expected pond capacity: %d, got: %d", 2, stat.PondCapacity)
	}
}

func TestManager_GetStat(t *testing.T) {
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

	stat := manager.GetStat()
	if stat.ReceivedCount != 1 {
		t.Errorf("expected received count: %d, got: %d", 1, stat.ReceivedCount)
	}
	if stat.GroupCapacity != 1 {
		t.Errorf("expected group capacity: %d, got: %d", 1, stat.GroupCapacity)
	}
}
