package main

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const reconcileSQL = `
SELECT dataset_release_transfer_id
  FROM dataset_release_transfer
 WHERE transfer_status IN ('queued', 'in_progress')
 ORDER BY dataset_release_transfer_id
`

// queryer is satisfied by both *pgxpool.Pool and *pgx.Conn so the same
// scan loop can run against either.
type queryer interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func enqueueStale(ctx context.Context, q queryer, pending chan<- int64) error {
	rows, err := q.Query(ctx, reconcileSQL)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return err
		}
		select {
		case pending <- id:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return rows.Err()
}

func runListener(ctx context.Context, dsn string, pending chan<- int64, log *slog.Logger) {
	for ctx.Err() == nil {
		err := listenOnce(ctx, dsn, pending, log)
		if err != nil && ctx.Err() == nil {
			log.Warn("listener connection lost; retrying in 5s", "err", err)
			select {
			case <-time.After(5 * time.Second):
			case <-ctx.Done():
				return
			}
		}
	}
}

func listenOnce(ctx context.Context, dsn string, pending chan<- int64, log *slog.Logger) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	if _, err := conn.Exec(ctx, "LISTEN "+NotifyChannel); err != nil {
		return err
	}
	log.Info("listening", "channel", NotifyChannel)

	// NOTIFY is not durable: rescan on every (re)connect so anything that
	// arrived while we were down still gets picked up.
	if err := enqueueStale(ctx, conn, pending); err != nil {
		return err
	}

	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		tid, perr := strconv.ParseInt(n.Payload, 10, 64)
		if perr != nil {
			log.Warn("malformed NOTIFY payload", "payload", n.Payload)
			continue
		}
		select {
		case pending <- tid:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func runReconciler(ctx context.Context, pool *pgxpool.Pool, pending chan<- int64, log *slog.Logger) {
	t := time.NewTicker(ReconcileInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := enqueueStale(ctx, pool, pending); err != nil && ctx.Err() == nil {
				log.Warn("reconcile rescan failed", "err", err)
			}
		}
	}
}
