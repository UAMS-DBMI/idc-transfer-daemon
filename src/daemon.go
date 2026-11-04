package main

import (
	"context"
	"fmt"
	"log/slog"
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
	lockBackoff         = 500 * time.Millisecond // doubles per retry: about 15s in all
	lockReleaseTimeout  = 30 * time.Second
)

// loadPoolConfig builds the pool config from the standard libpq environment
// (PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE, PGSSLMODE, PGPASSFILE,
// PGSERVICE, ...). pgx applies the same defaults and precedence as libpq when
// handed an empty connection string, so the daemon carries no DSN of its own
// and anything that works for psql works here.
func loadPoolConfig() (*pgxpool.Config, error) {
	cfg, err := pgxpool.ParseConfig("")
	if err != nil {
		return nil, fmt.Errorf("postgres config from PG* environment: %w", err)
	}
	cfg.MinConns = PoolMin
	cfg.MaxConns = PoolMax
	return cfg, nil
}

func run(ctx context.Context, log *slog.Logger) error {
	cfg, err := loadPoolConfig()
	if err != nil {
		return err
	}
	log.Info("postgres target",
		"host", cfg.ConnConfig.Host,
		"port", cfg.ConnConfig.Port,
		"database", cfg.ConnConfig.Database,
		"user", cfg.ConnConfig.User,
	)

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
	// The LISTEN connection lives outside the pool; give it its own copy of the
	// connection config so neither side can mutate the other's.
	listenCfg := cfg.ConnConfig.Copy()
	go func() { defer infra.Done(); runListener(ctx, listenCfg, pending, log) }()
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
