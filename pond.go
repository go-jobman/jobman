package jobman

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/1set/gut/yrand"
	rg "github.com/avast/retry-go"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"gopkg.in/fifo.v0"
)

// Pond represents a job processing unit with a queue and a pool of workers.
// It manages the lifecycle, logging, and counters for various job states.
type Pond struct {
	// basic
	mu        sync.RWMutex
	ctx       context.Context
	cancel    context.CancelFunc
	lg        *zap.SugaredLogger
	isClosed  bool
	id        string
	isShared  bool
	queueSize int
	poolSize  int
	// core
	queue     *fifo.Queue[*AllocatedJob]
	pool      *ants.Pool
	extQueues []*fifo.Queue[*AllocatedJob]
	watchOnce sync.Once
	// counter
	cntRecv  atomic.Int64
	cntEnque atomic.Int64
	cntDeque atomic.Int64
	cntStart atomic.Int64
	cntDone  atomic.Int64
}

// NewPartitionPond creates a new partition pond with the specified queue and pool size.
func NewPartitionPond(id string, queueSize, poolSize int) *Pond {
	ctx, cl := context.WithCancel(context.Background())
	pd := &Pond{
		lg:        log.With("pond", id),
		ctx:       ctx,
		cancel:    cl,
		id:        id,
		isShared:  false,
		queueSize: queueSize,
		poolSize:  poolSize,
		queue:     fifo.New[*AllocatedJob](queueSize),
		pool:      createPool(poolSize),
	}
	pd.lg.Debugw("new partition pond created", "queue_size", queueSize, "pool_size", poolSize)
	return pd
}

// NewSharedPond creates a new shared pond with the specified queue and pool size.
func NewSharedPond(id string, queueSize, poolSize int) *Pond {
	ctx, cl := context.WithCancel(context.Background())
	pd := &Pond{
		lg:        log.With("pond", id),
		ctx:       ctx,
		cancel:    cl,
		id:        id,
		isShared:  true,
		queueSize: queueSize,
		poolSize:  poolSize,
		queue:     fifo.New[*AllocatedJob](queueSize),
		pool:      createPool(poolSize),
		extQueues: make([]*fifo.Queue[*AllocatedJob], 0),
	}
	pd.lg.Debugw("new shared pond created", "queue_size", queueSize, "pool_size", poolSize)
	return pd
}

func (p *Pond) String() string {
	return fmt.Sprintf("🗳️Pond[%s](Shared:%s,Queue:%d,Pool:%d)",
		p.id,
		charBool(p.isShared),
		p.queueSize,
		p.poolSize,
	)
}

// GetID returns the ID of the pond.
func (p *Pond) GetID() string {
	return p.id
}

// GetQueue returns the queue of the pond.
func (p *Pond) GetQueue() *fifo.Queue[*AllocatedJob] {
	return p.queue
}

// GetPool returns the pool of the pond.
func (p *Pond) GetPool() *ants.Pool {
	return p.pool
}

// ListExternalQueues returns the external queues of the pond.
func (p *Pond) ListExternalQueues() []*fifo.Queue[*AllocatedJob] {
	p.mu.RLock()
	defer p.mu.RUnlock()
	ques := make([]*fifo.Queue[*AllocatedJob], len(p.extQueues))
	copy(ques, p.extQueues)
	return ques
}

// Close closes the pond and releases all resources.
func (p *Pond) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.isClosed = true
	p.cancel()
	_ = p.queue.Close()
	p.pool.Release()
}

// ResizeQueue resizes the queue of the pond.
func (p *Pond) ResizeQueue(newSize int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// if the pond is closed, do nothing
	if p.isClosed {
		return
	}
	// do nothing if the size is not changed
	if newSize == p.queueSize {
		return
	}

	l := p.lg.With("old_size", p.queueSize, "new_size", newSize)
	if err := p.queue.Resize(newSize); err != nil {
		l.Errorw("queue resize failed", zap.Error(err))
		return
	}

	p.queueSize = newSize
	l.Debug("queue resized")
	return
}

// ResizePool resizes the pool of the pond.
func (p *Pond) ResizePool(newSize int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// if the pond is closed, do nothing
	if p.isClosed {
		return
	}
	// do nothing if the size is not changed
	if newSize == p.poolSize {
		return
	}

	l := p.lg.With("old_size", p.poolSize, "new_size", newSize)
	p.pool.Tune(newSize)
	p.poolSize = newSize
	l.Debug("pool resized")
}

// Subscribe subscribes a queue to the list of external queues.
func (p *Pond) Subscribe(q *fifo.Queue[*AllocatedJob]) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// do nothing if the pond is closed
	if p.isClosed {
		return
	}
	p.extQueues = append(p.extQueues, q)
}

// Submit submits a job to the pond. If blockingCallback is true, the job's callback will be executed in the same goroutine.
func (p *Pond) Submit(j Job, blockingCallback bool) error {
	if j == nil {
		return ErrJobNil
	} else if p.isClosed {
		return ErrPondClosed
	}

	// basic
	l := p.lg.With("method", "submit", "job_id", j.ID())
	l.Debug("submit job")

	// create a job allocation
	idx := p.cntRecv.Inc()
	ja := &AllocatedJob{
		readyProc: make(chan struct{}),
		PondIndex: idx,
		SubmitAt:  time.Now(),
		Job:       j,
	}

	// attempt to enqueue the job, fail if the queue is full
	if err := p.queue.TryEnqueue(ja); err != nil {
		l.Warnw("enqueue failed", zap.Error(err))
		p.notifyJob(ja, blockingCallback, false)
		return err
	}

	// record the enqueue count
	l.Debugw("job enqueued", "enqueue_count", p.cntEnque.Inc())

	// notify the job is accepted
	p.notifyJob(ja, blockingCallback, true)

	// return nil if the job is submitted successfully
	return nil
}

// notifyJob notifies the job of acceptance or rejection.
func (p *Pond) notifyJob(ja *AllocatedJob, blockingCallback bool, accepted bool) {
	j := ja.Job
	if blockingCallback {
		if accepted {
			j.OnAccepted()
		} else {
			j.OnRejected()
		}
		close(ja.readyProc)
	} else {
		go func() {
			defer close(ja.readyProc)
			if accepted {
				j.OnAccepted()
			} else {
				j.OnRejected()
			}
		}()
	}
}

// StartPartitionWatchAsync starts the pond watch loop asynchronously.
func (p *Pond) StartPartitionWatchAsync() {
	if p.isClosed {
		return // do nothing if the pond is closed
	}
	p.watchOnce.Do(func() {
		go p.startPartitionWatch()
	})
}

// StartSharedWatchAsync starts the pond watch loop for own and all external queues asynchronously.
func (p *Pond) StartSharedWatchAsync() {
	if p.isClosed {
		return // do nothing if the pond is closed
	}
	p.watchOnce.Do(func() {
		go p.startSharedWatch()
	})
}

// startJobSubmissionLoop starts the job submission loop.
func (p *Pond) startJobSubmissionLoop(l *zap.SugaredLogger, done <-chan struct{}, jc <-chan *AllocatedJob, pl *ants.Pool) {
	for {
		select {
		case <-done:
			l.Debug("submit done")
			return
		case ja, ok := <-jc:
			if !ok {
				return
			}
			ll := l.With("job_id", ja.Job.ID())
			if err := pl.Submit(func() {
				ll.Debugw("☑️ job starts in pool", "start_count", p.cntStart.Inc(), "queue_time", time.Since(ja.SubmitAt))
				<-ja.readyProc
				ll.Debugw("🚀 job is ready to proceed in pool")
				ja.Job.Proceed()
				ll.Debugw("🏁 job completes in pool", "done_count", p.cntDone.Inc())
			}); err != nil {
				ll.Warnw("⚠️ failed to submit job to pool", zap.Error(err))
			}
		}
	}
}

func (p *Pond) startPartitionWatch() {
	l := p.lg.With("method", "own_watch")
	jc := make(chan *AllocatedJob)
	dc := p.ctx.Done()

	// start the watch loop to take a job from the queue for each time
	go func(done <-chan struct{}, jc chan<- *AllocatedJob, q *fifo.Queue[*AllocatedJob]) {
		defer close(jc)
		rd := 0
		for {
			rd++
			ll := l.With("round", rd)

			select {
			case <-done:
				l.Debugw("watch done", "round", rd)
				return
			default:
				if ja, err := q.Dequeue(); err == nil {
					ll.Debugw("partition job dequeued", "job_id", ja.Job.ID(), "dequeue_count", p.cntDeque.Inc())
					jc <- ja
				} else {
					ll.Warnw("partition job dequeue failed", zap.Error(err))
				}
			}
		}
	}(dc, jc, p.queue)

	// start the working loop to submit the job
	go p.startJobSubmissionLoop(l.With("pond_type", "partition"), dc, jc, p.pool)
}

func (p *Pond) startSharedWatch() {
	l := p.lg.With("method", "all_watch")
	jc := make(chan *AllocatedJob)
	dc := p.ctx.Done()
	sleep := func() {
		time.Sleep(SharedPondCheckInterval)
	}

	// start the watch loop to take a job from the queue for each time
	go func(done <-chan struct{}, jc chan<- *AllocatedJob, q *fifo.Queue[*AllocatedJob]) {
		defer close(jc)

		rd := 0
		for {
			rd++
			ll := l.With("round", rd)

			select {
			case <-done:
				ll.Debugw("watch done")
				return
			default:
				var (
					ja  *AllocatedJob
					err error
				)
				if ef := fixedRetry(func() error {
					ja, err = q.TryDequeue()
					return err
				}, SharedPondDequeueRetryLimit, SharedPondDequeueRetryInterval); ef == nil {
					ll.Debugw("shared job dequeued", "job_id", ja.Job.ID(), "dequeue_count", p.cntDeque.Inc())
					jc <- ja
				} else {
					// if got no left worker for external queues, sleep for a while
					if p.pool.Free() <= 0 {
						//ll.Debugw("no left worker for external queues")
						sleep()
						continue
					}

					// check the external queues
					p.mu.RLock()
					outs := make([]*fifo.Queue[*AllocatedJob], len(p.extQueues))
					copy(outs, p.extQueues)
					p.mu.RUnlock()

					// if no external queues, sleep for a while
					if len(outs) == 0 {
						//ll.Debugw("no external queues to check")
						sleep()
						continue
					}

					// shuffle the external queues
					_ = yrand.Shuffle(len(outs), func(i, j int) {
						outs[i], outs[j] = outs[j], outs[i]
					})

					// pick one job from each external queue
					jobCnt := 0
					for idx, out := range outs {
						if ja, err := out.TryDequeue(); err == nil {
							jobCnt++
							ll.Debugw("external partition job dequeued", "job_id", ja.Job.ID(), "queue_idx", idx, "dequeue_count", p.cntDeque.Inc())
							jc <- ja
						}
					}

					// if no job dequeued from external queues, sleep for a while
					if jobCnt == 0 {
						//ll.Debugw("no external partition job dequeued")
						sleep()
						continue
					}
				}
			}
		}
	}(dc, jc, p.queue)

	// start the working loop to submit the job
	go p.startJobSubmissionLoop(l.With("pond_type", "shared"), dc, jc, p.pool)
}

func createPool(size int) *ants.Pool {
	pl, _ := ants.NewPool(size, ants.WithNonblocking(false))
	return pl
}

func combinePondID(ids ...string) string {
	if len(ids) == 0 {
		return "_pond_"
	}
	if len(ids) == 1 {
		return ids[0] + "|_shared_"
	}
	return ids[0] + "|" + ids[1]
}

func charBool(yes bool) string {
	if yes {
		return "✔"
	}
	return "✘"
}

// fixedRetry retries to execute given function with consistent same delay.
func fixedRetry(work func() error, times uint, delay time.Duration) error {
	return rg.Do(
		work,
		rg.Attempts(times),
		rg.DelayType(rg.FixedDelay),
		rg.Delay(delay),
		rg.LastErrorOnly(true),
	)
}
