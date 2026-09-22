// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package worker // import "miniflux.app/v2/internal/worker"

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"miniflux.app/v2/internal/model"
)

// newTestPool creates a pool without any workers so tests can control the
// queue lifecycle directly.
func newTestPool(queueSize int) *Pool {
	ctx, cancel := context.WithCancel(context.Background())
	return &Pool{
		queue:  make(chan model.Job, queueSize),
		ctx:    ctx,
		cancel: cancel,
	}
}

func TestPoolShutdownGraceful(t *testing.T) {
	pool := NewPool(nil, 3)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pool.Shutdown(ctx); err != nil {
		t.Fatalf("expected graceful shutdown, got error: %v", err)
	}
}

func TestPoolShutdownIsIdempotent(t *testing.T) {
	pool := NewPool(nil, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pool.Shutdown(ctx); err != nil {
		t.Fatalf("first shutdown failed: %v", err)
	}
	if err := pool.Shutdown(ctx); err != nil {
		t.Fatalf("second shutdown should be a no-op, got error: %v", err)
	}
}

func TestPoolPushAfterShutdownDoesNotPanic(t *testing.T) {
	pool := NewPool(nil, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pool.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	// Pushing jobs after the shutdown must be a no-op instead of panicking
	// with "send on closed channel".
	pool.Push(model.JobList{
		{UserID: 1, FeedID: 1, FeedURL: "https://example.com/feed.xml"},
	})
}

func TestPoolPushShutdownRace(t *testing.T) {
	pool := newTestPool(64)

	jobs := model.JobList{
		{UserID: 1, FeedID: 1, FeedURL: "https://example.com/1.xml"},
		{UserID: 1, FeedID: 2, FeedURL: "https://example.com/2.xml"},
	}

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				pool.Push(jobs)
			}
		}()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pool.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	wg.Wait()
}

func TestPoolPushUnblockedOnShutdown(t *testing.T) {
	// Unbuffered queue with no workers: Push blocks until the pool context
	// is canceled by Shutdown.
	pool := newTestPool(0)

	done := make(chan struct{})
	go func() {
		defer close(done)
		pool.Push(model.JobList{
			{UserID: 1, FeedID: 1, FeedURL: "https://example.com/feed.xml"},
		})
	}()

	// Give Push a chance to block on the queue.
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pool.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown failed: %v", err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Push should have been unblocked by Shutdown")
	}
}

func TestPoolShutdownTimeout(t *testing.T) {
	pool := newTestPool(0)

	// Simulate a worker that never finishes its job.
	pool.wg.Add(1)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := pool.Shutdown(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Shutdown should have timed out quickly, took %v", elapsed)
	}

	// Release the simulated stuck worker so the waiting goroutine can exit.
	pool.wg.Done()
}

func TestWorkerStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)

	w := &worker{id: 0, store: nil}
	queue := make(chan model.Job)
	go w.Run(ctx, queue, &wg)

	cancel()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("worker should have stopped after context cancellation")
	}
}

func TestWorkerStopsWhenQueueClosed(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)

	w := &worker{id: 0, store: nil}
	queue := make(chan model.Job)
	go w.Run(context.Background(), queue, &wg)

	close(queue)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("worker should have stopped after the queue was closed")
	}
}
