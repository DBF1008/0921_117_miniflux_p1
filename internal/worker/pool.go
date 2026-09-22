// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package worker // import "miniflux.app/v2/internal/worker"

import (
	"context"
	"sync"
	"time"

	"miniflux.app/v2/internal/model"
	"miniflux.app/v2/internal/storage"
)

// ErrPoolClosed is returned by Push when the pool has already been shut down.
var ErrPoolClosed = errPoolClosed{}

type errPoolClosed struct{}

func (errPoolClosed) Error() string { return "worker: pool is closed" }

// Pool manages a set of background workers that process feed refresh jobs.
type Pool struct {
	queue chan model.Job

	// mu protects "closed" and serializes Push against Shutdown so that a
	// send can never race with close(queue).
	mu     sync.RWMutex
	closed bool

	// ctx is cancelled when Shutdown starts. Workers pass it to running jobs
	// so that in-flight feed refreshes can be interrupted.
	ctx    context.Context
	cancel context.CancelFunc

	wg sync.WaitGroup
}

// Push sends a list of jobs to the queue. It returns ErrPoolClosed if the
// pool has already been shut down.
func (p *Pool) Push(jobs model.JobList) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return ErrPoolClosed
	}

	for _, job := range jobs {
		// Holding the read lock for the whole batch makes the "closed" check
		// atomic with the sends: Shutdown cannot close the queue while a send
		// is in progress because it needs the write lock.
		select {
		case p.queue <- job:
		case <-p.ctx.Done():
			// Shutdown was requested: stop enqueueing instead of blocking
			// forever on a queue that is about to be closed.
			return ErrPoolClosed
		}
	}
	return nil
}

// Shutdown cancels in-flight jobs, closes the job queue and waits up to
// timeout for all workers to finish. It returns true if all workers stopped
// within the timeout, false otherwise. A zero timeout waits indefinitely.
// Shutdown is idempotent and safe to call concurrently with Push.
func (p *Pool) Shutdown(timeout time.Duration) bool {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return p.waitWorkers(timeout)
	}
	p.closed = true
	p.cancel()
	close(p.queue)
	p.mu.Unlock()

	return p.waitWorkers(timeout)
}

func (p *Pool) waitWorkers(timeout time.Duration) bool {
	if timeout <= 0 {
		p.wg.Wait()
		return true
	}

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// NewPool creates a pool of background workers.
func NewPool(store *storage.Storage, nbWorkers int) *Pool {
	ctx, cancel := context.WithCancel(context.Background())
	workerPool := &Pool{
		queue:  make(chan model.Job),
		ctx:    ctx,
		cancel: cancel,
	}

	for i := range nbWorkers {
		workerPool.wg.Add(1)
		worker := &worker{id: i, store: store}
		go worker.Run(ctx, workerPool.queue, &workerPool.wg)
	}

	return workerPool
}
