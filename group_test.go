package jobman_test

import (
	"testing"

	"gopkg.in/jobman.v0"
)

func TestNewGroup(t *testing.T) {
	group := jobman.NewGroup("test-group", 10, 5)
	if group == nil {
		t.Fatal("expected group to be created, got nil")
	}

	if group.String() != "🗂️Group[test-group](Ponds:1,Received:0,Enqueued:0)" {
		t.Errorf("unexpected string representation: %s", group.String())
	}

	if group.GetID() != "test-group" {
		t.Errorf("expected group ID: %s, got: %s", "test-group", group.GetID())
	}
}

func TestGroup_InitializePond(t *testing.T) {
	group := jobman.NewGroup("test-group", 10, 5)
	group.InitializePond("partition-1", 5, 3)

	if group.GetPond("partition-1").GetQueue().Len() != 0 {
		t.Errorf("expected empty queue")
	}
}

func TestGroup_GetPond(t *testing.T) {
	group := jobman.NewGroup("test-group", 10, 5)

	sharedPond := group.GetPond("")
	if sharedPond == nil {
		t.Fatal("expected shared pond, got nil")
	}

	group.InitializePond("partition-1", 5, 3)
	partPond := group.GetPond("partition-1")
	if partPond == nil {
		t.Fatal("expected partition pond, got nil")
	}
}
