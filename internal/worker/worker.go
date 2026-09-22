// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package worker // import "miniflux.app/v2/internal/worker"

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"miniflux.app/v2/internal/config"
	"miniflux.app/v2/internal/metric"
	"miniflux.app/v2/internal/model"
	feedHandler "miniflux.app/v2/internal/reader/handler"
	"miniflux.app/v2/internal/storage"
)

type worker struct {
	id    int
	store *storage.Storage
}

// executeJob is the function invoked by workers to refresh a feed. It is a
// package-level variable so tests can replace it and observe context
// cancellation without a database.
var executeJob = func(ctx context.Context, store *storage.Storage, userID, feedID int64) bool {
	return feedHandler.RefreshFeed(ctx, store, userID, feedID, false) == nil
}

// Run processes feed refresh jobs from the channel until it is closed or the
// context is cancelled.
func (w *worker) Run(ctx context.Context, c <-chan model.Job, wg *sync.WaitGroup) {
	defer wg.Done()

	slog.Debug("Worker started",
		slog.Int("worker_id", w.id),
	)

	for {
		select {
		case <-ctx.Done():
			slog.Debug("Worker stopped",
				slog.Int("worker_id", w.id),
			)
			return
		case job, ok := <-c:
			if !ok {
				slog.Debug("Worker stopped",
					slog.Int("worker_id", w.id),
				)
				return
			}

			slog.Debug("Job received by worker",
				slog.Int("worker_id", w.id),
				slog.Int64("user_id", job.UserID),
				slog.Int64("feed_id", job.FeedID),
				slog.String("feed_url", job.FeedURL),
			)

			startTime := time.Now()
			success := executeJob(ctx, w.store, job.UserID, job.FeedID)

			if config.Opts.HasMetricsCollector() {
				status := metric.StatusSuccess
				if !success {
					status = metric.StatusError
				}
				metric.BackgroundFeedRefreshDuration.WithLabelValues(status).Observe(time.Since(startTime).Seconds())
			}
		}
	}
}
