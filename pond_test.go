package jobman_test

import (
	"testing"
	"time"

	"gopkg.in/jobman.v0"
)

func TestNewPartitionPond(t *testing.T) {
	pond := jobman.NewPartitionPond("test-pond1", 10, 5)
	if pond == nil {
		t.Fatal("expected pond to be created, got nil")
	}

	if pond.String() != "🗳️Pond[test-pond1](Shared:✘,Queue:10,Pool:5)" {
		t.Errorf("unexpected string representation: %s", pond.String())
	}

	if pond.GetID() != "test-pond1" {
		t.Errorf("expected pond ID: %s, got: %s", "test-pond", pond.GetID())
	}
}

func TestNewSharedPond(t *testing.T) {
	pond := jobman.NewSharedPond("test-pond0", 10, 5)
	if pond == nil {
		t.Fatal("expected pond to be created, got nil")
	}

	if pond.String() != "🗳️Pond[test-pond0](Shared:✔,Queue:10,Pool:5)" {
		t.Errorf("unexpected string representation: %s", pond.String())
	}

	if pond.GetID() != "test-pond0" {
		t.Errorf("expected pond ID: %s, got: %s", "test-pond", pond.GetID())
	}
}

func TestPond_ResizeQueue(t *testing.T) {
	pond := jobman.NewPartitionPond("test-pond", 10, 5)

	// Resize to a larger size
	pond.ResizeQueue(20)
	if pond.GetQueue().Cap() != 20 {
		t.Errorf("expected queue capacity: %d, got: %d", 20, pond.GetQueue().Cap())
	}

	// Resize to a smaller size
	pond.ResizeQueue(5)
	if pond.GetQueue().Cap() != 5 {
		t.Errorf("expected queue capacity: %d, got: %d", 5, pond.GetQueue().Cap())
	}

	// Resize to the same size (no-op)
	pond.ResizeQueue(5)
	if pond.GetQueue().Cap() != 5 {
		t.Errorf("expected queue capacity: %d, got: %d", 5, pond.GetQueue().Cap())
	}

	// Resize to invalid size (no-op)
	pond.ResizeQueue(0)
	if pond.GetQueue().Cap() != 5 {
		t.Errorf("expected queue capacity: %d, got: %d", 5, pond.GetQueue().Cap())
	}

	// Resize a closed pond
	pond.Close()
	pond.ResizeQueue(15)
	if pond.GetQueue().Cap() != 5 { // Should remain the same as before close
		t.Errorf("expected queue capacity after close: %d, got: %d", 5, pond.GetQueue().Cap())
	}
}

func TestPond_ResizePool(t *testing.T) {
	pond := jobman.NewPartitionPond("test-pond", 10, 5)

	// Resize to a larger size
	pond.ResizePool(10)
	if pond.GetPool().Cap() != 10 {
		t.Errorf("expected pool capacity: %d, got: %d", 10, pond.GetPool().Cap())
	}

	// Resize to a smaller size
	pond.ResizePool(3)
	if pond.GetPool().Cap() != 3 {
		t.Errorf("expected pool capacity: %d, got: %d", 3, pond.GetPool().Cap())
	}

	// Resize to the same size (no-op)
	pond.ResizePool(3)
	if pond.GetPool().Cap() != 3 {
		t.Errorf("expected pool capacity: %d, got: %d", 3, pond.GetPool().Cap())
	}

	// Resize to invalid size (no-op)
	pond.ResizePool(0)
	if pond.GetPool().Cap() != 3 {
		t.Errorf("expected pool capacity: %d, got: %d", 3, pond.GetPool().Cap())
	}

	// Resize a closed pond
	pond.Close()
	pond.ResizePool(7)
	if pond.GetPool().Cap() != 3 { // Should remain the same as before close
		t.Errorf("expected pool capacity after close: %d, got: %d", 3, pond.GetPool().Cap())
	}
}

func TestPond_Submit(t *testing.T) {
	pond := jobman.NewPartitionPond("test-pond", 10, 5)
	job := &MockJob{id: "job1"}

	if err := pond.Submit(job); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	time.Sleep(50 * time.Millisecond) // allow some time for the handler to proceed
	if !job.IsAccepted() {
		t.Error("expected job to be accepted")
	}
}

func TestPond_Close(t *testing.T) {
	pond := jobman.NewPartitionPond("test-pond", 10, 5)
	pond.Close()

	job := &MockJob{id: "job1"}
	if err := pond.Submit(job); err != jobman.ErrPondClosed {
		t.Fatalf("expected error: %v, got: %v", jobman.ErrPondClosed, err)
	}
}

func TestPond_SubmitNilJob(t *testing.T) {
	pond := jobman.NewPartitionPond("test-pond", 10, 5)
	err := pond.Submit(nil)
	if err != jobman.ErrJobNil {
		t.Fatalf("expected error: %v, got: %v", jobman.ErrJobNil, err)
	}
}

func TestPond_SubmitToClosedPond(t *testing.T) {
	pond := jobman.NewPartitionPond("test-pond", 10, 5)
	pond.Close()
	job := &MockJob{id: "job1"}
	err := pond.Submit(job)
	if err != jobman.ErrPondClosed {
		t.Fatalf("expected error: %v, got: %v", jobman.ErrPondClosed, err)
	}
}

func TestPond_SubmitJobToFullQueue(t *testing.T) {
	pond := jobman.NewPartitionPond("test-pond", 1, 5) // Small queue size for testing
	job1 := &MockJob{id: "job1"}
	job2 := &MockJob{id: "job2"}

	if err := pond.Submit(job1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := pond.Submit(job2); err == nil {
		t.Fatal("expected queue full error, got nil")
	}
}

func TestPond_Subscribe(t *testing.T) {
	sharedPond := jobman.NewSharedPond("shared-pond", 10, 5)
	partPond := jobman.NewPartitionPond("part-pond", 10, 5)
	sharedPond.Subscribe(partPond.GetQueue())

	if sharedPond.GetQueue().Len() != 0 {
		t.Fatal("expected internal queue to be empty initially")
	}
	if l := len(sharedPond.GetExternalQueues()); l != 1 {
		t.Fatalf("expected external queue to be subscribed, got: %d", l)
	}
}

func TestPond_StartPartitionWatchAsync(t *testing.T) {
	pond := jobman.NewPartitionPond("test-pond", 10, 5)
	pond.StartPartitionWatchAsync()
	job := &MockJob{id: "job1"}

	if err := pond.Submit(job); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	time.Sleep(50 * time.Millisecond) // allow some time for the handler to proceed
	if !job.IsAccepted() {
		t.Error("expected job to be accepted")
	}
}

func TestPond_StartSharedWatchAsync(t *testing.T) {
	sharedPond := jobman.NewSharedPond("shared-pond", 10, 5)
	partPond := jobman.NewPartitionPond("part-pond", 10, 5)
	sharedPond.Subscribe(partPond.GetQueue())

	sharedPond.StartSharedWatchAsync()
	job := &MockJob{id: "job1"}

	if err := partPond.Submit(job); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	time.Sleep(50 * time.Millisecond) // allow some time for the handler to proceed
	if !job.IsAccepted() {
		t.Error("expected job to be accepted")
	}
}

func TestPond_GetSharedPondQueue(t *testing.T) {
	sharedPond := jobman.NewSharedPond("test-shared-pond", 10, 5)
	partitionPond := jobman.NewPartitionPond("test-partition-pond", 5, 3)

	sharedPond.Subscribe(partitionPond.GetQueue())

	if l := len(sharedPond.GetExternalQueues()); l != 1 {
		t.Errorf("expected 1 external queue, got: %d", l)
	}
}
