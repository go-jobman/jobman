package jobman_test

import (
	"errors"
	"testing"

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

	blockForHandling() // allow some time for the handler to proceed
	if !job.IsAccepted() {
		t.Error("expected job to be accepted")
	}
}

func TestPond_Close(t *testing.T) {
	pond := jobman.NewPartitionPond("test-pond", 10, 5)
	pond.Close()

	job := &MockJob{id: "job1"}
	if err := pond.Submit(job); !errors.Is(err, jobman.ErrPondClosed) {
		t.Fatalf("expected error: %v, got: %v", jobman.ErrPondClosed, err)
	}
}

func TestPond_SubmitNilJob(t *testing.T) {
	pond := jobman.NewPartitionPond("test-pond", 10, 5)
	err := pond.Submit(nil)
	if !errors.Is(err, jobman.ErrJobNil) {
		t.Fatalf("expected error: %v, got: %v", jobman.ErrJobNil, err)
	}
}

func TestPond_SubmitToClosedPond(t *testing.T) {
	pond := jobman.NewPartitionPond("test-pond", 10, 5)
	pond.Close()
	job := &MockJob{id: "job1"}
	err := pond.Submit(job)
	if !errors.Is(err, jobman.ErrPondClosed) {
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
	if l := len(sharedPond.ListExternalQueues()); l != 1 {
		t.Fatalf("expected external queue to be subscribed, got: %d", l)
	}

	sharedPond.Close()
	anotherPond := jobman.NewPartitionPond("another-pond", 10, 5)
	sharedPond.Subscribe(anotherPond.GetQueue())
	if l := len(sharedPond.ListExternalQueues()); l != 1 {
		t.Fatalf("expected no new external queue to be subscribed, got: %d", l)
	}
}

func TestPond_StartPartitionWatchAsync(t *testing.T) {
	pond := jobman.NewPartitionPond("test-pond", 10, 5)
	pond.StartPartitionWatchAsync()
	job := &MockJob{id: "job1"}

	if err := pond.Submit(job); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	blockForHandling() // allow some time for the handler to proceed
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

	blockForHandling() // allow some time for the handler to proceed
	if !job.IsAccepted() {
		t.Error("expected job to be accepted")
	}
}

func TestPond_GetSharedPondQueue(t *testing.T) {
	sharedPond := jobman.NewSharedPond("test-shared-pond", 10, 5)
	partitionPond := jobman.NewPartitionPond("test-partition-pond", 5, 3)

	sharedPond.Subscribe(partitionPond.GetQueue())

	if l := len(sharedPond.ListExternalQueues()); l != 1 {
		t.Errorf("expected 1 external queue, got: %d", l)
	}
}

func TestStartSharedWatch_BasicFunctionality(t *testing.T) {
	sharedPond := jobman.NewSharedPond("shared-pond", 10, 5)
	sharedPond.StartSharedWatchAsync()

	job := &MockJob{id: "job1"}
	err := sharedPond.Submit(job)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	blockForHandling() // Allow some time for the handler to proceed

	if !job.IsAccepted() {
		t.Error("expected job to be accepted")
	}
	if !job.IsProceeded() {
		t.Error("expected job to be proceeded")
	}
}

func TestStartSharedWatch_ExternalQueuesHandling(t *testing.T) {
	sharedPond := jobman.NewSharedPond("shared-pond", 10, 5)
	partPond := jobman.NewPartitionPond("part-pond", 10, 5)
	sharedPond.Subscribe(partPond.GetQueue())
	sharedPond.StartSharedWatchAsync()

	job := &MockJob{id: "job1"}
	err := partPond.Submit(job)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	blockForHandling() // Allow some time for the handler to proceed

	if !job.IsAccepted() {
		t.Error("expected job to be accepted")
	}
	if !job.IsProceeded() {
		t.Error("expected job to be proceeded")
	}
}

func TestStartSharedWatch_ConcurrentSubmissions(t *testing.T) {
	sharedPond := jobman.NewSharedPond("shared-pond", 10, 5)
	sharedPond.StartSharedWatchAsync()

	job1 := &MockJob{id: "job1"}
	job2 := &MockJob{id: "job2"}

	err := sharedPond.Submit(job1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err = sharedPond.Submit(job2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	blockForHandling() // Allow some time for the handler to proceed

	if !job1.IsAccepted() {
		t.Error("expected job1 to be accepted")
	}
	if !job1.IsProceeded() {
		t.Error("expected job1 to be proceeded")
	}
	if !job2.IsAccepted() {
		t.Error("expected job2 to be accepted")
	}
	if !job2.IsProceeded() {
		t.Error("expected job2 to be proceeded")
	}
}

func TestStartSharedWatch_EmptyExternalQueues(t *testing.T) {
	sharedPond := jobman.NewSharedPond("shared-pond", 10, 5)
	partPond1 := jobman.NewPartitionPond("part-pond1", 10, 5)
	partPond2 := jobman.NewPartitionPond("part-pond2", 10, 5)
	sharedPond.Subscribe(partPond1.GetQueue())
	sharedPond.Subscribe(partPond2.GetQueue())
	sharedPond.StartSharedWatchAsync()

	job := &MockJob{id: "job1"}
	err := sharedPond.Submit(job)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	blockForHandling() // Allow some time for the handler to proceed

	if !job.IsAccepted() {
		t.Error("expected job to be accepted")
	}
	if !job.IsProceeded() {
		t.Error("expected job to be proceeded")
	}
}

func TestStartSharedWatch_PartitionPondsFull(t *testing.T) {
	sharedPond := jobman.NewSharedPond("shared-pond", 5, 5)
	partPond1 := jobman.NewPartitionPond("part-pond1", 1, 1) // Small queue size for testing
	partPond2 := jobman.NewPartitionPond("part-pond2", 1, 1) // Small queue size for testing
	sharedPond.Subscribe(partPond1.GetQueue())
	sharedPond.Subscribe(partPond2.GetQueue())
	sharedPond.StartSharedWatchAsync()

	job1 := &MockJob{id: "job1"}
	job2 := &MockJob{id: "job2"}
	job3 := &MockJob{id: "job3"}
	job4 := &MockJob{id: "job4"}
	job5 := &MockJob{id: "job5"}
	job6 := &MockJob{id: "job6"}
	job7 := &MockJob{id: "job7"}

	// Fill partition ponds
	err := partPond1.Submit(job1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err = partPond1.Submit(job2)
	if err == nil {
		t.Fatal("expected queue full error, got nil")
	}
	err = partPond2.Submit(job3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err = partPond2.Submit(job4)
	if err == nil {
		t.Fatal("expected queue full error, got nil")
	}

	blockForHandling() // Allow some time for the handler to proceed

	// Submit jobs to partition pond later
	err = partPond1.Submit(job5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err = partPond2.Submit(job6)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err = sharedPond.Submit(job7)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	blockForHandling() // Allow some time for the handler to proceed

	if !job1.IsAccepted() {
		t.Error("expected job1 to be accepted")
	}
	if !job1.IsProceeded() {
		t.Error("expected job1 to be proceeded")
	}
	if !job3.IsAccepted() {
		t.Error("expected job3 to be accepted")
	}
	if !job3.IsProceeded() {
		t.Error("expected job3 to be proceeded")
	}
	if !job5.IsAccepted() {
		t.Error("expected job5 to be accepted")
	}
	if !job5.IsProceeded() {
		t.Error("expected job5 to be proceeded")
	}
	if !job6.IsAccepted() {
		t.Error("expected job6 to be accepted")
	}
	if !job6.IsProceeded() {
		t.Error("expected job6 to be proceeded")
	}
	if !job7.IsAccepted() {
		t.Error("expected job7 to be accepted")
	}
	if !job7.IsProceeded() {
		t.Error("expected job7 to be proceeded")
	}
}

func TestStartSharedWatch_SharedPondsFull(t *testing.T) {
	sharedPond := jobman.NewSharedPond("shared-pond", 3, 1)
	partPond1 := jobman.NewPartitionPond("part-pond1", 1, 1) // Small queue size for testing
	partPond2 := jobman.NewPartitionPond("part-pond2", 1, 1) // Small queue size for testing
	sharedPond.Subscribe(partPond1.GetQueue())
	sharedPond.Subscribe(partPond2.GetQueue())
	sharedPond.StartSharedWatchAsync()

	job1 := &MockJob{id: "job1"}
	job2 := &MockJob{id: "job2"}
	job3 := &MockJob{id: "job3"}
	job4 := &MockJob{id: "job4"}
	job5 := &MockJob{id: "job5"}
	job6 := &MockJob{id: "job6"}
	job7 := &MockJob{id: "job7"}
	job8 := &MockJob{id: "job8"}
	job9 := &MockJob{id: "job9"}
	job10 := &MockJob{id: "job10"}

	// Submit jobs to shared pond to make it busy
	err := sharedPond.Submit(job5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err = sharedPond.Submit(job6)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err = sharedPond.Submit(job7)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = sharedPond.Submit(job8) // this job will be rejected or accepted based on the handling speed

	// Fill partition ponds
	err = partPond1.Submit(job1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err = partPond1.Submit(job2)
	if err == nil {
		t.Fatal("expected queue full error, got nil")
	}
	err = partPond2.Submit(job3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err = partPond2.Submit(job4)
	if err == nil {
		t.Fatal("expected queue full error, got nil")
	}

	// Start partition watch
	partPond1.StartPartitionWatchAsync()
	partPond2.StartPartitionWatchAsync()

	blockForHandling() // Allow some time for the handler to proceed

	// Submit additional jobs to partition ponds after shared pond is busy
	err = partPond1.Submit(job9)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err = partPond2.Submit(job10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	blockForHandling() // Allow some time for the handler to proceed

	// Check statuses of all jobs
	if !job1.IsAccepted() || !job1.IsProceeded() {
		t.Error("expected job1 to be accepted and proceeded")
	}
	if !job3.IsAccepted() || !job3.IsProceeded() {
		t.Error("expected job3 to be accepted and proceeded")
	}
	if !job5.IsAccepted() || !job5.IsProceeded() {
		t.Error("expected job5 to be accepted and proceeded")
	}
	if !job6.IsAccepted() || !job6.IsProceeded() {
		t.Error("expected job6 to be accepted and proceeded")
	}
	if !job7.IsAccepted() || !job7.IsProceeded() {
		t.Error("expected job7 to be accepted and proceeded")
	}
	if !job9.IsAccepted() {
		t.Error("expected job9 to be accepted")
	}
	if !job10.IsAccepted() {
		t.Error("expected job10 to be accepted")
	}
}
