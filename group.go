package jobman

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// Group represents a collection of ponds, including a shared pond and partition ponds.
// It manages the context, logger, and counters for received and enqueued items.
type Group struct {
	// basic
	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
	lg     *zap.SugaredLogger
	id     string
	// core
	sharedPond *Pond
	partPonds  map[string]*Pond
	// counters
	cntRecv  atomic.Int64
	cntEnque atomic.Int64
}

// NewGroup creates a new group of ponds.
func NewGroup(id string, sharedQueue, sharedPool int) *Group {
	ctx, cancel := context.WithCancel(context.Background())
	sp := NewSharedPond(getPondName(id), sharedQueue, sharedPool)
	sp.StartSharedWatchAsync()
	return &Group{
		ctx:        ctx,
		cancel:     cancel,
		lg:         log.With("group", id),
		id:         id,
		sharedPond: sp,
		partPonds:  make(map[string]*Pond),
	}
}

func (g *Group) String() string {
	return fmt.Sprintf(
		"🗂️Group[%s](Ponds:%d,Received:%d,Enqueued:%d)",
		g.id,
		len(g.partPonds)+1, // add 1 for shared pond
		g.cntRecv.Load(),
		g.cntEnque.Load(),
	)
}

// GetID returns the id of the group.
func (g *Group) GetID() string {
	return g.id
}

// GetPond returns the pond in the group for the given partition. If partition is empty, it returns the shared pond.
func (g *Group) GetPond(partition string) *Pond {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if partition == "" {
		return g.sharedPond
	}
	return g.partPonds[partition]
}

// InitializePond initializes the pond for the given partition.
func (g *Group) InitializePond(partition string, queueSize, poolSize int) {
	g.mu.Lock()
	defer g.mu.Unlock()

	// empty partition means shared pond, which is already initialized
	if partition == "" {
		return
	}

	// partition pond already exists
	if _, ok := g.partPonds[partition]; ok {
		return
	}

	// create a new partition pond
	pd := NewPartitionPond(getPondName(g.id, partition), queueSize, poolSize)
	pd.StartPartitionWatchAsync()

	// save the partition pond
	g.sharedPond.Subscribe(pd.GetQueue())
	g.partPonds[partition] = pd
}
