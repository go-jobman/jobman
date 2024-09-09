package jobman

import (
	"bitbucket.org/ai69/amoy"
	"context"
	"fmt"
	"github.com/1set/gut/yrand"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"gopkg.in/fifo.v0"
	"sync"
	"time"
)

type Pond struct {
	// basic
	sync.RWMutex
	ctx       context.Context
	cancel    context.CancelFunc
	lg        *zap.SugaredLogger
	name      string
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

func (p *Pond) String() string {
	return fmt.Sprintf("🗳️Pond{Name:%s Queue:%d Pool:%d Shared:%t}", p.name, p.queueSize, p.poolSize, p.isShared)
}

func createPool(size int) *ants.Pool {
	pl, _ := ants.NewPool(size, ants.WithNonblocking(false))
	return pl
}

// NewPartitionPond creates a new partition pond with the specified queue and pool size.
func NewPartitionPond(name string, queueSize, poolSize int) *Pond {
	ctx, cl := context.WithCancel(context.Background())
	pd := &Pond{
		lg:        log.With("pond", name),
		ctx:       ctx,
		cancel:    cl,
		name:      name,
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
func NewSharedPond(name string, queueSize, poolSize int) *Pond {
	ctx, cl := context.WithCancel(context.Background())
	pd := &Pond{
		lg:        log.With("pond", name),
		ctx:       ctx,
		cancel:    cl,
		name:      name,
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

// Close closes the pond and releases all resources.
func (p *Pond) Close() {
	p.Lock()
	defer p.Unlock()

	p.cancel()
	_ = p.queue.Close()
	p.pool.Release()
}

// GetID returns the ID of the pond.
func (p *Pond) GetID() string {
	return p.name
}

// ResizeQueue resizes the queue of the pond.
func (p *Pond) ResizeQueue(newSize int) {
	p.Lock()
	defer p.Unlock()

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
	p.Lock()
	defer p.Unlock()

	// do nothing if the size is not changed
	if newSize == p.poolSize {
		return
	}

	l := p.lg.With("old_size", p.poolSize, "new_size", newSize)
	p.pool.Tune(newSize)
	p.poolSize = newSize
	l.Debug("pool resized")
}

// Submit submits a job to the pond.
func (p *Pond) Submit(j Job) error {
	if j == nil {
		return ErrJobNil
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

		// notify the job is rejected
		go func() {
			defer close(ja.readyProc)
			if j.OnRejected != nil {
				j.OnRejected()
			}
		}()

		return err
	}

	// record the enqueue count
	l.Debugw("job enqueued", "enqueue_count", p.cntEnque.Load())
	p.cntEnque.Inc()

	// notify the job is accepted
	go func() {
		defer close(ja.readyProc)
		if j.OnAccepted != nil {
			j.OnAccepted()
		}
	}()

	// return nil if the job is submitted successfully
	return nil
}

// Subscribe subscribes a queue to the list of external queues.
func (p *Pond) Subscribe(q *fifo.Queue[*AllocatedJob]) {
	p.Lock()
	defer p.Unlock()

	p.extQueues = append(p.extQueues, q)
}

// GetQueue returns the queue of the pond.
func (p *Pond) GetQueue() *fifo.Queue[*AllocatedJob] {
	return p.queue
}

// GetPool returns the pool of the pond.
func (p *Pond) GetPool() *ants.Pool {
	return p.pool
}

// StartPartitionWatchAsync starts the pond watch loop asynchronously.
func (p *Pond) StartPartitionWatchAsync() {
	p.watchOnce.Do(func() {
		go p.startPartitionWatch()
	})
}

// StartSharedWatchAsync starts the pond watch loop for own and all external queues asynchronously.
func (p *Pond) StartSharedWatchAsync() {
	p.watchOnce.Do(func() {
		go p.startSharedWatch()
	})
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
	go func(done <-chan struct{}, jc <-chan *AllocatedJob, pl *ants.Pool) {
		for {
			select {
			case <-done:
				l.Debug("submit done")
				return
			case ja, ok := <-jc:
				if !ok {
					return
				}
				jid := ja.Job.ID()
				if err := pl.Submit(func() {
					l.Debugw("✅ partition job starts in partition pool", "job_id", jid, "start_count", p.cntStart.Inc(), "queue_time", time.Since(ja.SubmitAt))
					<-ja.readyProc
					if j := ja.Job; j.Proceed != nil {
						j.Proceed()
					}
					l.Debugw("🏁 partition job completes in partition pool", "job_id", jid, "done_count", p.cntDone.Inc())
				}); err != nil {
					l.Warnw("⚠️ failed to submit partition job to partition pool", "job_id", jid, zap.Error(err))
				}
			}
		}
	}(dc, jc, p.pool)
}

func (p *Pond) startSharedWatch() {
	l := p.lg.With("method", "all_watch")
	jc := make(chan *AllocatedJob)
	dc := p.ctx.Done()

	// start the watch loop to take a job from the queue for each time
	go func(done <-chan struct{}, jc chan<- *AllocatedJob, q *fifo.Queue[*AllocatedJob]) {
		defer close(jc)
		sleep := func() {
			time.Sleep(SharedPondCheckInterval)
		}

		rd := 0
		for {
			rd++
			ll := l.With("round", rd)

			select {
			case <-done:
				l.Debugw("watch done", "round", rd)
				return
			default:
				var (
					ja  *AllocatedJob
					err error
				)
				if ef := amoy.FixedRetry(func() error {
					ja, err = q.TryDequeue()
					return err
				}, SharedPondDequeueRetryLimit, SharedPondDequeueRetryInterval); ef == nil {
					ll.Debugw("shared job dequeued", "job_id", ja.Job.ID(), "dequeue_count", p.cntDeque.Inc())
					jc <- ja
				} else {
					// if got no left worker for external queues, sleep for a while
					if p.pool.Free() <= 0 {
						ll.Debugw("no left worker for external queues")
						sleep()
						continue
					}

					// check the external queues
					p.RLock()
					outs := make([]*fifo.Queue[*AllocatedJob], len(p.extQueues))
					copy(outs, p.extQueues)
					p.RUnlock()

					// if no external queues, sleep for a while
					if len(outs) == 0 {
						ll.Debugw("no external queues to check")
						sleep()
						continue
					}

					// shuffle the external queues
					_ = yrand.Shuffle(len(outs), func(i, j int) {
						outs[i], outs[j] = outs[j], outs[i]
					})

					// check each external queue for only one job
					jobCnt := 0
					for idx, out := range outs {
						if ja, err := out.TryDequeue(); err == nil {
							jobCnt++
							ll.Debugw("external tenant job dequeued", "job_id", ja.Job.ID(), "queue_idx", idx, "dequeue_count", p.cntDeque.Inc())
							jc <- ja
						}
					}

					// if no job dequeued from external queues, sleep for a while
					if jobCnt == 0 {
						ll.Debugw("no external tenant job dequeued")
						sleep()
						continue
					}
				}
			}
		}
	}(dc, jc, p.queue)

	// start the working loop to submit the job
	go func(done <-chan struct{}, jc <-chan *AllocatedJob, pl *ants.Pool) {
		for {
			select {
			case <-done:
				l.Debug("submit done")
				return
			case ja, ok := <-jc:
				if !ok {
					return
				}

				jid := ja.Job.ID()
				if err := pl.Submit(func() {
					l.Debugw("☑️ job starts in shared pool", "job_id", jid, "start_count", p.cntStart.Inc())
					<-ja.readyProc
					if j := ja.Job; j.Proceed != nil {
						j.Proceed()
					}
					l.Debugw("🏁 job completes in shared pool", "job_id", jid, "done_count", p.cntDone.Inc())
				}); err != nil {
					l.Warnw("⚠️ failed to submit job to shared pool", "job_id", jid, zap.Error(err))
				}
			}
		}
	}(dc, jc, p.pool)
}
