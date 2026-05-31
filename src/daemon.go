package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	Concurrency         = 200                    // per-transfer worker count
	MaxAttempts         = 3                      // inline upload retries per file
	BackoffBase         = 500 * time.Millisecond // exponential backoff base
	FlushEvery          = 500                    // flush after this many rows
	FlushAfter          = 2 * time.Second        // ...or this long after the first buffered row
	PoolMin             = int32(2)
	PoolMax             = int32(6)
	TransferParallelism = 1               // at most one transfer in flight
	ReconcileInterval   = 5 * time.Minute // safety-net rescan for missed NOTIFYs
	NotifyChannel       = "idc_transfer_channel"
)

func run(ctx context.Context, log *slog.Logger) error {
	dsn := os.Getenv("TRANSFER_DAEMON_DSN")
	if dsn == "" {
		return errors.New("TRANSFER_DAEMON_DSN must be set")
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return err
	}
	cfg.MinConns = PoolMin
	cfg.MaxConns = PoolMax

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer pool.Close()

	gcs, err := newGCSClient(ctx)
	if err != nil {
		return err
	}
	defer gcs.Close()

	pending := make(chan int64, 1024)
	sem := make(chan struct{}, TransferParallelism)

	var infra sync.WaitGroup
	infra.Add(2)
	go func() { defer infra.Done(); runListener(ctx, dsn, pending, log) }()
	go func() { defer infra.Done(); runReconciler(ctx, pool, pending, log) }()

	var inflight sync.Map
	var transfers sync.WaitGroup

dispatch:
	for {
		select {
		case <-ctx.Done():
			break dispatch
		case tid := <-pending:
			if _, dup := inflight.LoadOrStore(tid, struct{}{}); dup {
				continue
			}
			transfers.Add(1)
			go func(id int64) {
				defer transfers.Done()
				defer inflight.Delete(id)

				select {
				case sem <- struct{}{}:
				case <-ctx.Done():
					return
				}
				defer func() { <-sem }()

				if err := processTransfer(ctx, pool, gcs, id, log); err != nil {
					log.Error("transfer crashed", "transfer_id", id, "err", err)
				}
			}(tid)
		}
	}

	log.Info("shutdown requested; draining in-flight transfer(s)")
	infra.Wait()
	transfers.Wait()
	return nil
}
