package jobman

import (
	"context"
	"sync"

	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type Group struct {
	// basic
	sync.RWMutex
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

func getPondName(ids ...string) string {
	if len(ids) == 0 {
		return "_pond_"
	}
	if len(ids) == 1 {
		return ids[0] + "|_shared_"
	}
	return ids[0] + "|" + ids[1]
}

// InitializePartitionPond initializes the pond for the given partition.
func (g *Group) InitializePartitionPond(partition string, queueSize, poolSize int) {
	g.Lock()
	defer g.Unlock()

	// empty partition means shared pond
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
	g.RLock()
	defer g.RUnlock()

	if partition == "" {
		return g.sharedPond
	}
	return g.partPonds[partition]
}
