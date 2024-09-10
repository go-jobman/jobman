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
	name   string
	// core
	sharedPond *Pond
	partPonds  map[string]*Pond
	// counters
	cntRecv  atomic.Int64
	cntEnque atomic.Int64
}

// NewGroup creates a new group of ponds.
func NewGroup(name string, sharedQueue, sharedPool int) *Group {
	ctx, cancel := context.WithCancel(context.Background())
	sp := NewSharedPond(getPondName(name), sharedQueue, sharedPool)
	sp.StartSharedWatchAsync()
	return &Group{
		ctx:        ctx,
		cancel:     cancel,
		lg:         log.With("group", name),
		name:       name,
		sharedPond: sp,
		partPonds:  make(map[string]*Pond),
	}
}

func (g *Group) String() string {
	return fmt.Sprintf(
		"🗂️Group[%s](Ponds:%d,Received:%d,Enqueued:%d)",
		g.name,
		len(g.partPonds)+1, // add 1 for shared pond
		g.cntRecv.Load(),
		g.cntEnque.Load(),
	)
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
	pd := NewPartitionPond(getPondName(g.name, partition), queueSize, poolSize)
	pd.StartPartitionWatchAsync()

	// save the partition pond
	g.sharedPond.Subscribe(pd.GetQueue())
	g.partPonds[partition] = pd
}

// GetPond returns the pond for the given partition.
func (g *Group) GetPond(partition string) *Pond {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if partition == "" {
		return g.sharedPond
	}
	return g.partPonds[partition]
}
