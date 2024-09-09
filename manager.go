package jobman

import (
	"go.uber.org/zap"
	"sync"
)

// Hub is the main entry point for submitting batch jobs.
type Hub struct {
	sync.RWMutex
	lg     *zap.SugaredLogger
	name   string
	alloc  AllocatorFunc
	groups map[string]*Group
}

// NewHub creates a new Hub with the specified name.
func NewHub(name string) *Hub {
	return &Hub{
		lg:   log.With("hub", name),
		name: name,
		alloc: func(group, partition string) (Allocation, error) {
			return Allocation{
				GroupID:   group,
				PondID:    partition,
				IsShared:  partition == "",
				QueueSize: 2,
				PoolSize:  1,
			}, nil
		},
		groups: make(map[string]*Group),
	}
}

// SetAllocator sets the allocator function for the hub.
func (h *Hub) SetAllocator(a AllocatorFunc) {
	h.Lock()
	defer h.Unlock()

	h.alloc = a
}

// Submit submits a job to the pond of the specified group in the hub.
func (h *Hub) Submit(j Job) error {
	if j == nil {
		return ErrJobNil
	}

	// basic
	l := h.lg.With(zap.String("method", "submit"), zap.String("job_id", j.ID()))
	l.Debug("try to submit job")

	// get identifier for job
	al, err := h.alloc(j.Group(), j.Partition())
	if err != nil {
		l.Warnw("allocation failed", zap.Error(err))
		return err
	}
	l.Debugw("got allocation", "allocation", al)

	// lock for the group
	h.Lock()
	defer h.Unlock()

	// find the group for the job or create a new group
	grp, ok := h.groups[al.GroupID]
	if !ok {
		l.Debugw("creating new group", "group_id", al.GroupID)
		// get shared pool allocation
		var sl Allocation
		if al.IsShared {
			sl = al
		} else {
			if sl, err = h.alloc(j.Group(), ""); err != nil {
				l.Warnw("allocation for shared failed", zap.Error(err))
				return err
			}
		}
		grp = NewGroup(sl.GroupID, sl.QueueSize, sl.PoolSize)
		h.groups[sl.GroupID] = grp
	}

	// initialize the pool if not shared
	if !al.IsShared {
		grp.InitializePond(al.PondID, al.QueueSize, al.PoolSize)
	}

	// get the pond and submit the job
	pd := grp.GetPond(al.PondID)
	if pd == nil {
		l.Warnw("pond not found", "pond_id", al.PondID)
		return err
	}
	if err := pd.Submit(j); err != nil {
		l.Warnw("dispatch failed", zap.Error(err))
		return err
	}

	// if accepted, save in db and hub, and return
	// TODO: save in DB

	return nil
}

// findPond is a helper method to find the pond via group and tenant.
func (h *Hub) findPond(group, tenant string) (*Pond, error) {
	h.RLock()
	defer h.RUnlock()

	grp, ok := h.groups[group]
	if !ok {
		log.Warnw("group not found", "group", group)
		return nil, ErrGroupNotFound
	}
	pd := grp.GetPond(tenant)
	if pd == nil {
		log.Warnw("pond not found", "group", group, "pond", tenant)
		return nil, ErrPondNotFound
	}
	return pd, nil
}

// ResizeQueue resizes the queue of the pond in the specified group.
func (h *Hub) ResizeQueue(group, tenant string, newSize int) error {
	pd, err := h.findPond(group, tenant)
	if err != nil {
		return err
	}
	pd.ResizeQueue(newSize)
	return nil
}

// ResizePool resizes the pool of the pond in the specified group.
func (h *Hub) ResizePool(group, tenant string, newSize int) error {
	pd, err := h.findPond(group, tenant)
	if err != nil {
		return err
	}
	pd.ResizePool(newSize)
	return nil
}
