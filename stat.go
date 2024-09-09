package jobman

// PondStat represents the statistics of a pond.
type PondStat struct {
	ReceivedCount  int64 `json:"job_received"`
	EnqueuedCount  int64 `json:"job_enqueued"`
	DequeuedCount  int64 `json:"job_dequeued"`
	ProceededCount int64 `json:"job_proceeded"`
	CompletedCount int64 `json:"job_completed"`
	QueueCapacity  int   `json:"queue_cap"`
	QueueFree      int   `json:"queue_free"`
	PoolCapacity   int   `json:"pool_cap"`
	PoolFree       int   `json:"pool_free"`
}

// GetStat returns the statistics of the pond.
func (p *Pond) GetStat() *PondStat {
	qc, ql := p.queue.Cap(), p.queue.Len()
	qf := qc - ql
	if qf < 0 {
		qf = 0
	}
	return &PondStat{
		ReceivedCount:  p.cntRecv.Load(),
		EnqueuedCount:  p.cntEnque.Load(),
		DequeuedCount:  p.cntDeque.Load(),
		ProceededCount: p.cntStart.Load(),
		CompletedCount: p.cntDone.Load(),
		QueueCapacity:  qc,
		QueueFree:      qf,
		PoolCapacity:   p.pool.Cap(),
		PoolFree:       p.pool.Free(),
	}
}

// GroupStat represents the statistics of a group.
type GroupStat struct {
	ReceivedCount int64                `json:"job_received"`
	EnqueuedCount int64                `json:"job_enqueued"`
	PondCapacity  int                  `json:"pond_cap"`
	PondStats     map[string]*PondStat `json:"pond_stats"`
}

// GetStat returns the statistics of the group.
func (g *Group) GetStat() *GroupStat {
	g.RLock()
	defer g.RUnlock()

	pc := len(g.partPonds) + 1 // add 1 for shared pond
	ps := make(map[string]*PondStat, pc)
	for k, v := range g.partPonds {
		ps[k] = v.GetStat()
	}
	ps["_shared_"] = g.sharedPond.GetStat()
	return &GroupStat{
		ReceivedCount: g.cntRecv.Load(),
		EnqueuedCount: g.cntEnque.Load(),
		PondCapacity:  pc,
		PondStats:     ps,
	}
}

// ManagerStat represents the statistics of a manager.
type ManagerStat struct {
	ReceivedCount int64                 `json:"job_received"`
	GroupCapacity int                   `json:"group_cap"`
	GroupStats    map[string]*GroupStat `json:"group_stats"`
}

// GetStat returns the statistics of the manager.
func (m *Manager) GetStat() *ManagerStat {
	m.RLock()
	defer m.RUnlock()

	gs := make(map[string]*GroupStat, len(m.groups))
	for k, v := range m.groups {
		gs[k] = v.GetStat()
	}
	return &ManagerStat{
		ReceivedCount: m.cntRecv.Load(),
		GroupCapacity: len(m.groups),
		GroupStats:    gs,
	}
}
