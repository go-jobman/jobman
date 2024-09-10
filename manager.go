package jobman

import (
	"fmt"
	"sync"

	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// Manager is the main entry point for submitting jobs.
type Manager struct {
	mu     sync.RWMutex
	lg     *zap.SugaredLogger
	name   string
	alloc  AllocatorFunc
	groups map[string]*Group
	// counters
	cntRecv atomic.Int64
}

// NewManager creates a new Manager with the specified name.
func NewManager(name string) *Manager {
	return &Manager{
		lg:   log.With("manager", name),
		name: name,
		alloc: func(group, partition string) (Allocation, error) {
			// default allocation: 2 queue size, 1 pool size
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

func (m *Manager) String() string {
	return fmt.Sprintf(
		"📨Manager[%s](Groups:%d,Received:%d)",
		m.name,
		len(m.groups),
		m.cntRecv.Load(),
	)
}

// SetAllocator sets the allocator function for the manager.
func (m *Manager) SetAllocator(a AllocatorFunc) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.alloc = a
}

// ResizeQueue resizes the queue of the pond in the specified group.
func (m *Manager) ResizeQueue(group, partition string, newSize int) error {
	pd, err := m.GetPond(group, partition)
	if err != nil {
		return err
	}
	pd.ResizeQueue(newSize)
	return nil
}

// ResizePool resizes the pool of the pond in the specified group.
func (m *Manager) ResizePool(group, partition string, newSize int) error {
	pd, err := m.GetPond(group, partition)
	if err != nil {
		return err
	}
	pd.ResizePool(newSize)
	return nil
}

// GetPond is a helper method to find the pond via group and partition.
func (m *Manager) GetPond(group, partition string) (*Pond, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	grp, ok := m.groups[group]
	if !ok {
		m.lg.Warnw("group not found", "group", group)
		return nil, ErrGroupNotFound
	}
	pd := grp.GetPond(partition)
	if pd == nil {
		m.lg.Warnw("pond not found", "group", group, "partition", partition)
		return nil, ErrPondNotFound
	}
	return pd, nil
}

// GetGroup is a helper method to find the group via group id.
func (m *Manager) GetGroup(group string) (*Group, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	grp, ok := m.groups[group]
	if !ok {
		m.lg.Warnw("group not found", "group", group)
		return nil, ErrGroupNotFound
	}
	return grp, nil
}

// Dispatch submits a job to the pond of the specified group in the manager.
func (m *Manager) Dispatch(j Job) error {
	_, err := m.DispatchWithAllocation(j)
	return err
}

// DispatchWithAllocation submits a job to the pond of the specified group in the manager and returns the allocation result and error.
func (m *Manager) DispatchWithAllocation(j Job) (*Allocation, error) {
	if j == nil {
		return nil, ErrJobNil
	}

	// basic
	mgrIdx := m.cntRecv.Inc()
	l := m.lg.With(zap.String("method", "dispatch"), zap.Int64("mgr_idx", mgrIdx), zap.String("job_id", j.ID()))
	l.Debug("try to dispatch job")

	// get allocation for the job
	if m.alloc == nil {
		l.Warn("allocator not set")
		return nil, ErrAllocatorNotSet
	}
	al, err := m.alloc(j.Group(), j.Partition())
	if err != nil {
		l.Warnw("allocation failed", zap.Error(err))
		return nil, err
	}
	l.Debugw("got allocation", "allocation", al)

	// lock for the group
	m.mu.Lock()
	defer m.mu.Unlock()

	// find the group for the job or create a new group
	grp, ok := m.groups[al.GroupID]
	if !ok {
		l.Debugw("creating new group", "group_id", al.GroupID)
		// get shared pool allocation
		var sl Allocation
		if al.IsShared {
			sl = al
		} else {
			if sl, err = m.alloc(j.Group(), ""); err != nil {
				l.Warnw("allocation for shared failed", zap.Error(err))
				return nil, err
			}
		}
		grp = NewGroup(sl.GroupID, sl.QueueSize, sl.PoolSize)
		m.groups[sl.GroupID] = grp
	}

	// initialize the pool if not shared
	if !al.IsShared {
		grp.InitializePond(al.PondID, al.QueueSize, al.PoolSize)
	}
	grp.cntRecv.Inc()

	// get the pond and submit the job
	if al.IsShared {
		al.PondID = "" // reset the pond id for the shared pool
	}
	pd := grp.GetPond(al.PondID)
	if pd == nil {
		l.Warnw("pond not found", "group_id", al.GroupID, "pond_id", al.PondID) // it should not happen
		return nil, ErrPondNotFound
	}
	if err := pd.Submit(j); err != nil {
		l.Warnw("job dispatch failed", "group_id", al.GroupID, "pond_id", al.PondID, zap.Error(err))
		return nil, err
	}

	// success
	l.Infow("job dispatched successfully", "group_id", al.GroupID, "pond_id", al.PondID, "group_count", grp.cntEnque.Inc())
	return &al, nil
}
