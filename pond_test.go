package jobman_test

import (
	"testing"
	"time"

	"gopkg.in/jobman.v0"
)

func TestNewPartitionPond(t *testing.T) {
	pond := jobman.NewPartitionPond("test-pond", 10, 5)
	if pond == nil {
		t.Fatal("expected pond to be created, got nil")
	}

	if pond.String() != "🗳️Pond[test-pond](Shared:✘,Queue:10,Pool:5)" {
		t.Errorf("unexpected string representation: %s", pond.String())
	}

	if pond.GetID() != "test-pond" {
		t.Errorf("expected pond ID: %s, got: %s", "test-pond", pond.GetID())
	}
}

func TestNewSharedPond(t *testing.T) {
	pond := jobman.NewSharedPond("test-pond", 10, 5)
	if pond == nil {
		t.Fatal("expected pond to be created, got nil")
	}

	if pond.String() != "🗳️Pond[test-pond](Shared:✔,Queue:10,Pool:5)" {
		t.Errorf("unexpected string representation: %s", pond.String())
	}
}

func TestPond_ResizeQueue(t *testing.T) {
	pond := jobman.NewPartitionPond("test-pond", 10, 5)

	pond.ResizeQueue(20)
	if pond.GetQueue().Cap() != 20 {
		t.Errorf("expected queue capacity: %d, got: %d", 20, pond.GetQueue().Cap())
	}
}

func TestPond_ResizePool(t *testing.T) {
	pond := jobman.NewPartitionPond("test-pond", 10, 5)

	pond.ResizePool(10)
	if pond.GetPool().Cap() != 10 {
		t.Errorf("expected pool capacity: %d, got: %d", 10, pond.GetPool().Cap())
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

func TestPond_GetStat(t *testing.T) {
	pond := jobman.NewPartitionPond("test-pond", 10, 5)
	job := &MockJob{id: "job1"}

	pond.Submit(job)
	time.Sleep(100 * time.Millisecond) // Allow some time for the job to proceed

	stat := pond.GetStat()
	if stat.ReceivedCount != 1 {
		t.Errorf("expected received count: %d, got: %d", 1, stat.ReceivedCount)
	}
	if stat.EnqueuedCount != 1 {
		t.Errorf("expected enqueued count: %d, got: %d", 1, stat.EnqueuedCount)
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
		t.Fatal("expected external queue to be empty initially")
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
