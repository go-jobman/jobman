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
	sharedPond  *Pond
	tenantPonds map[string]*Pond
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
		ctx:         ctx,
		cancel:      cancel,
		lg:          log.With("group", name),
		name:        name,
		sharedPond:  sp,
		tenantPonds: make(map[string]*Pond),
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

// InitializeTenantPond initializes the pond for the given tenant.
func (g *Group) InitializeTenantPond(tenant string, queueSize, poolSize int) {
	g.Lock()
	defer g.Unlock()

	// empty tenant means shared pond
	if tenant == "" {
		return
	}

	// tenant pond already exists
	if _, ok := g.tenantPonds[tenant]; ok {
		return
	}

	// create a new tenant pond
	pd := NewPartitionPond(getPondName(g.name, tenant), queueSize, poolSize)
	pd.StartPartitionWatchAsync()

	// save the tenant pond
	g.sharedPond.Subscribe(pd.GetQueue())
	g.tenantPonds[tenant] = pd
}

// GetPond returns the pond for the given tenant.
func (g *Group) GetPond(tenant string) *Pond {
	g.RLock()
	defer g.RUnlock()

	if tenant == "" {
		return g.sharedPond
	}
	return g.tenantPonds[tenant]
}
