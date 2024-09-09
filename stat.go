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

	ps := make(map[string]*PondStat, len(g.partPonds)+1)
	for k, v := range g.partPonds {
		ps[k] = v.GetStat()
	}
	return &GroupStat{
		ReceivedCount: g.cntRecv.Load(),
		EnqueuedCount: g.cntEnque.Load(),
		PondCapacity:  len(g.partPonds) + 1, // add 1 for shared pond
		PondStats:     ps,
	}
}
