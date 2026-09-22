// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package worker

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"miniflux.app/v2/internal/config"
	"miniflux.app/v2/internal/model"
	"miniflux.app/v2/internal/storage"
)

func TestMain(m *testing.M) {
	config.Opts = config.NewConfigOptions()
	os.Exit(m.Run())
}

// stubJob swaps the package-level executeJob seam with fn and restores the
// previous implementation on cleanup.
func stubJob(t *testing.T, fn func(ctx context.Context)) {
	t.Helper()
	previous := executeJob
	t.Cleanup(func() { executeJob = previous })
	executeJob = func(ctx context.Context, _ *storage.Storage, _, _ int64) bool {
		fn(ctx)
		return true
	}
}

func TestPushAfterShutdownReturnsError(t *testing.T) {
	stubJob(t, func(context.Context) {})

	pool := NewPool(nil, 1)
	if ok := pool.Shutdown(0); !ok {
		t.Fatal("Shutdown with no pending jobs should succeed")
	}

	if err := pool.Push(model.JobList{{UserID: 1, FeedID: 1}}); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("Push after Shutdown should return ErrPoolClosed, got %v", err)
	}
}

func TestShutdownIsIdempotent(t *testing.T) {
	stubJob(t, func(context.Context) {})

	pool := NewPool(nil, 2)
	if !pool.Shutdown(time.Second) {
		t.Fatal("first Shutdown should succeed")
	}
	if !pool.Shutdown(time.Second) {
		t.Fatal("second Shutdown should also succeed")
	}
}

func TestPushAndShutdownConcurrentNoPanic(t *testing.T) {
	var executed atomic.Int64
	stubJob(t, func(context.Context) {
		executed.Add(1)
		time.Sleep(time.Millisecond)
	})

	pool := NewPool(nil, 4)

	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = pool.Push(model.JobList{{UserID: 1, FeedID: 1}, {UserID: 2, FeedID: 2}})
		}()
	}

	// Shutdown races with the producers; it must never panic on a closed channel.
	time.Sleep(2 * time.Millisecond)
	pool.Shutdown(5 * time.Second)
	wg.Wait()
}

func TestShutdownCancelsInFlightJob(t *testing.T) {
	started := make(chan struct{})
	stubJob(t, func(ctx context.Context) {
		close(started)
		<-ctx.Done()
	})

	pool := NewPool(nil, 1)
	if err := pool.Push(model.JobList{{UserID: 1, FeedID: 1}}); err != nil {
		t.Fatalf("Push failed: %v", err)
	}

	<-started

	done := make(chan struct{})
	go func() {
		pool.Shutdown(5 * time.Second)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Shutdown did not cancel the in-flight job")
	}
}

func TestShutdownReturnsFalseWhenJobStuck(t *testing.T) {
	started := make(chan struct{})
	stubJob(t, func(context.Context) {
		close(started)
		// Ignore cancellation: simulate a feed refresh that cannot be interrupted.
		time.Sleep(10 * time.Second)
	})

	pool := NewPool(nil, 1)
	if err := pool.Push(model.JobList{{UserID: 1, FeedID: 1}}); err != nil {
		t.Fatalf("Push failed: %v", err)
	}

	<-started

	if pool.Shutdown(100 * time.Millisecond) {
		t.Fatal("Shutdown should return false when a worker ignores cancellation")
	}
}
