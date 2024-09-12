package jobman_test

import (
	"testing"

	"gopkg.in/jobman.v0"
)

func TestPond_GetStat(t *testing.T) {
	pond := jobman.NewPartitionPond("test-pond", 10, 5)
	job := &MockJob{id: "job1"}

	pond.Submit(job, false)
	blockForHandling() // Allow some time for the job to proceed

	stat := pond.GetStat()
	if stat.ReceivedCount != 1 {
		t.Errorf("expected received count: %d, got: %d", 1, stat.ReceivedCount)
	}
	if stat.EnqueuedCount != 1 {
		t.Errorf("expected enqueued count: %d, got: %d", 1, stat.EnqueuedCount)
	}
	t.Logf("pond stat:\n%s", stat)
}

func TestGroup_GetStat(t *testing.T) {
	group := jobman.NewGroup("test-group", 10, 5)
	group.InitializePond("partition-1", 5, 3)
	group.InitializePond("partition-2", 5, 3)

	stat := group.GetStat()
	if stat.ReceivedCount != 0 {
		t.Errorf("expected received count: %d, got: %d", 0, stat.ReceivedCount)
	}
	if stat.EnqueuedCount != 0 {
		t.Errorf("expected enqueued count: %d, got: %d", 0, stat.EnqueuedCount)
	}
	if stat.PondCapacity != 3 {
		t.Errorf("expected pond capacity: %d, got: %d", 3, stat.PondCapacity)
	}
	t.Logf("group stat:\n%v", stat)
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
		t.Errorf("unexpected error: %v", err)
	}
	job2 := &MockJob{id: "job2", group: "group2"}
	if err := manager.Dispatch(job2); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	stat := manager.GetStat()
	if stat.ReceivedCount != 2 {
		t.Errorf("expected received count: %d, got: %d", 2, stat.ReceivedCount)
	}
	if stat.GroupCapacity != 2 {
		t.Errorf("expected group capacity: %d, got: %d", 2, stat.GroupCapacity)
	}
	t.Logf("manager stat:\n%v", stat)
}
