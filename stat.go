package jobman

import (
	"fmt"
	"sort"
	"strings"
)

const (
	emojiStat = "📊"
)

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

// String returns a string representation of the PondStat struct.
func (ps PondStat) String() string {
	return fmt.Sprintf(emojiStat+"PondStat(Received=%d,Enqueued=%d,Dequeued=%d,Proceeded=%d,Completed=%d,QueueCap=%d,QueueFree=%d,PoolCap=%d,PoolFree=%d)",
		ps.ReceivedCount,
		ps.EnqueuedCount,
		ps.DequeuedCount,
		ps.ProceededCount,
		ps.CompletedCount,
		ps.QueueCapacity,
		ps.QueueFree,
		ps.PoolCapacity,
		ps.PoolFree,
	)
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

func (gs GroupStat) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf(emojiStat+"GroupStat(Received=%d,Enqueued=%d,PondCapacity=%d):\n",
		gs.ReceivedCount,
		gs.EnqueuedCount,
		gs.PondCapacity,
	))
	sb.WriteString("  Ponds:\n")

	// Collect and sort pond names
	pondNames := make([]string, 0, len(gs.PondStats))
	for k := range gs.PondStats {
		pondNames = append(pondNames, k)
	}
	sort.Strings(pondNames)

	// Append sorted pond stats to the string builder
	for _, k := range pondNames {
		sb.WriteString(fmt.Sprintf("    %s ➡ %s\n", k, gs.PondStats[k]))
	}
	return sb.String()
}

// GetStat returns the statistics of the group.
func (g *Group) GetStat() *GroupStat {
	g.mu.RLock()
	defer g.mu.RUnlock()

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

func (ms ManagerStat) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf(emojiStat+"ManagerStat(Received=%d,GroupCapacity=%d):\n",
		ms.ReceivedCount,
		ms.GroupCapacity,
	))
	sb.WriteString("  Groups:\n")

	// Collect and sort group names
	groupNames := make([]string, 0, len(ms.GroupStats))
	for k := range ms.GroupStats {
		groupNames = append(groupNames, k)
	}
	sort.Strings(groupNames)

	// Append sorted group stats to the string builder
	for _, k := range groupNames {
		sb.WriteString(fmt.Sprintf("    %s ➡\n    %s", k, ms.GroupStats[k]))
	}
	return sb.String()
}

// GetStat returns the statistics of the manager.
func (m *Manager) GetStat() *ManagerStat {
	m.mu.RLock()
	defer m.mu.RUnlock()

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
