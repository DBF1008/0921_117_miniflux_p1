// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package worker // import "miniflux.app/v2/internal/worker"

import (
	"context"
	"sync"

	"miniflux.app/v2/internal/model"
	"miniflux.app/v2/internal/storage"
)

// Pool manages a set of background workers that process feed refresh jobs.
//
// The job queue is never closed: workers stop when the pool context is
// canceled. This avoids "send on closed channel" panics when Push runs
// concurrently with Shutdown.
type Pool struct {
	queue  chan model.Job
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.RWMutex
	closed bool
}

// Push sends a list of jobs to the queue.
// Jobs are dropped once the pool is shutting down.
func (p *Pool) Push(jobs model.JobList) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.closed {
		return
	}

	for _, job := range jobs {
		select {
		case p.queue <- job:
		case <-p.ctx.Done():
			return
		}
	}
}

// Shutdown stops the workers and waits for them to finish their current jobs.
//
// The pool context is canceled to interrupt in-progress jobs and unblock
// pending Push calls, then it blocks until all workers exit or the provided
// context expires. It returns the context error if the workers did not stop
// in time.
func (p *Pool) Shutdown(ctx context.Context) error {
	// Cancel first: interrupts in-progress jobs and unblocks Push calls
	// waiting to send on a full queue (they hold the read lock).
	p.cancel()

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.mu.Unlock()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
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
