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
