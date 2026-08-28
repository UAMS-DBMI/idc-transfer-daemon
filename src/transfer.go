package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
)

type fileRow struct {
	ID     int64 // transfer_file_id
	FileID int64
	// transfer_file.file_dest_url -- relative to transfer_idc.base_gcs_url
	// (e.g. "imaging/<md5>"), not a gs:// URL.
	DestPath  string
	LocalPath string
	MD5Hex    string
	Attempts  int32
}

type fileResult struct {
	ID       int64
	Status   string // "completed" | "failed"
	Error    string // empty when Status == "completed"
	Attempts int32
}

func processTransfer(ctx context.Context, pool *pgxpool.Pool, gcs *storage.Client, transferID int64, log *slog.Logger) error {
	claimed, err := claimTransfer(ctx, pool, transferID)
	if err != nil {
		return fmt.Errorf("claim: %w", err)
	}
	if !claimed {
		log.Info("transfer skipped (not claimable)", "transfer_id", transferID)
		return nil
	}
	log.Info("transfer starting", "transfer_id", transferID)

	// Every path in transfer_file is relative to this anchor, so read it once
	// per transfer rather than per row. Posda's queue gate rejects an empty
	// base_gcs_url, but the field stays editable in the Manage modal, so the
	// daemon cannot assume it is set: without it every object would land in a
	// bucket-less path.
	baseURL, err := loadBaseGCSURL(ctx, pool, transferID)
	if err != nil {
		return err
	}
	log.Info("resolved package anchor", "transfer_id", transferID, "base_gcs_url", baseURL)

	queue := make(chan fileRow, Concurrency*2)
	results := make(chan fileResult, FlushEvery*4)

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		defer close(queue)
		return fileProducer(gctx, pool, transferID, queue)
	})

	g.Go(func() error {
		cg, cgctx := errgroup.WithContext(gctx)
		for i := 0; i < Concurrency; i++ {
			cg.Go(func() error {
				return fileConsumer(cgctx, gcs, transferID, baseURL, queue, results, log)
			})
		}
		err := cg.Wait()
		close(results) // exactly once, after all consumers have returned
		return err
	})

	g.Go(func() error { return flusherLoop(gctx, pool, results) })

	if err := g.Wait(); err != nil {
		return fmt.Errorf("pipeline: %w", err)
	}

	var ok, notOk int64
	if err := pool.QueryRow(ctx, `
        SELECT COUNT(*) FILTER (WHERE status = 'completed'),
               COUNT(*) FILTER (WHERE status <> 'completed')
          FROM transfer_file
         WHERE dataset_release_transfer_id = $1
    `, transferID).Scan(&ok, &notOk); err != nil {
		return fmt.Errorf("tally: %w", err)
	}
	log.Info("transfer file results", "transfer_id", transferID, "completed", ok, "incomplete", notOk)

	if notOk > 0 || ok == 0 {
		// Any non-completed row blocks submission; ok==0 catches the empty case
		// (external fan-out didn't run, or recordset was empty).
		if _, err := pool.Exec(ctx, `
            UPDATE dataset_release_transfer
               SET transfer_status='failed', when_updated=now(),
                   who_updated=0 -- auth.users 0 = 'system'
             WHERE dataset_release_transfer_id=$1
        `, transferID); err != nil {
			return fmt.Errorf("mark failed: %w", err)
		}
		log.Warn("transfer marked failed", "transfer_id", transferID)
		return nil
	}

	manifestURLs, err := uploadManifests(ctx, pool, gcs, transferID, baseURL)
	if err != nil {
		return fmt.Errorf("upload manifests: %w", err)
	}

	// The manifest URL is deliberately not recorded. Manifest names are fixed
	// and sit directly under transfer_idc.base_gcs_url, so the URL is derivable.
	// It must not be written into base_gcs_url: that is the package anchor Posda
	// owns and every relative file path hangs off, so storing a manifest URL
	// there would repoint the whole package one level deeper.
	if _, err := pool.Exec(ctx, `
        UPDATE dataset_release_transfer
           SET transfer_status='success', when_updated=now(),
               who_updated=0 -- auth.users 0 = 'system'
         WHERE dataset_release_transfer_id=$1
    `, transferID); err != nil {
		return fmt.Errorf("mark success: %w", err)
	}
	log.Info("transfer succeeded", "transfer_id", transferID, "manifests", manifestURLs)
	return nil
}

func claimTransfer(ctx context.Context, pool *pgxpool.Pool, transferID int64) (bool, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	var got int64
	err = tx.QueryRow(ctx, `
        UPDATE dataset_release_transfer
           SET transfer_status = 'in_progress', when_updated = now(),
               who_updated = 0 -- auth.users 0 = 'system'
         WHERE dataset_release_transfer_id = $1
           AND transfer_status IN ('queued', 'in_progress')
        RETURNING dataset_release_transfer_id
    `, transferID).Scan(&got)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	// Fan-out into transfer_file already happened externally; on re-queue,
	// give previously failed files another pass.
	if _, err := tx.Exec(ctx, `
        UPDATE transfer_file
           SET status='pending', attempts=0, error=NULL, when_updated=now()
         WHERE dataset_release_transfer_id = $1 AND status = 'failed'
    `, transferID); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

func fileProducer(ctx context.Context, pool *pgxpool.Pool, transferID int64, queue chan<- fileRow) error {
	// Keyset pagination in short autocommit queries — no hours-long open
	// transaction pinning the xmin horizon while 500k rows churn.
	var lastID int64
	for {
		rows, err := pool.Query(ctx, `
            SELECT tif.transfer_file_id,
                   tif.file_id,
                   tif.file_dest_url,
                   fsr.root_path || '/' || fl.rel_path,
                   f.digest,
                   tif.attempts
              FROM transfer_file      tif
              JOIN file               f   ON f.file_id  = tif.file_id
              JOIN file_location      fl  ON fl.file_id = tif.file_id
              JOIN file_storage_root  fsr ON fsr.file_storage_root_id = fl.file_storage_root_id
             WHERE tif.dataset_release_transfer_id = $1
               AND tif.status = 'pending'
               AND tif.transfer_file_id > $2
             ORDER BY tif.transfer_file_id
             LIMIT 1000
        `, transferID, lastID)
		if err != nil {
			return err
		}
		var batch []fileRow
		for rows.Next() {
			var r fileRow
			if err := rows.Scan(&r.ID, &r.FileID, &r.DestPath, &r.LocalPath, &r.MD5Hex, &r.Attempts); err != nil {
				rows.Close()
				return err
			}
			batch = append(batch, r)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		if len(batch) == 0 {
			return nil
		}
		for _, r := range batch {
			select {
			case queue <- r:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		lastID = batch[len(batch)-1].ID
	}
}

func fileConsumer(ctx context.Context, gcs *storage.Client, transferID int64, baseURL string, queue <-chan fileRow, results chan<- fileResult, log *slog.Logger) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case row, ok := <-queue:
			if !ok {
				return nil
			}
			res := uploadOne(ctx, gcs, transferID, baseURL, row, log)
			select {
			case results <- res:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// uploadOne drives one file all the way to a terminal result. The producer
// streams each row exactly once, so there is no second pass to clean up
// anything left at 'pending' — inline retry is load-bearing.
func uploadOne(ctx context.Context, gcs *storage.Client, transferID int64, baseURL string, row fileRow, log *slog.Logger) fileResult {
	expectedMD5, err := decodeMD5Hex(row.MD5Hex)
	if err != nil {
		return fileResult{ID: row.ID, Status: "failed",
			Error: fmt.Sprintf("decode md5 digest: %v", err), Attempts: row.Attempts + 1}
	}
	// DestPath is relative, so the anchor supplies the bucket and the package
	// folder. parseGSURL splits at the first slash after gs://, so the rest --
	// "<slug>/v<n>/imaging/<md5>" -- is the object name, slashes included.
	bucket, object, err := parseGSURL(baseURL + "/" + row.DestPath)
	if err != nil {
		return fileResult{ID: row.ID, Status: "failed", Error: err.Error(), Attempts: row.Attempts + 1}
	}

	attempts := row.Attempts
	var lastErr error
	for attempt := 1; attempt <= MaxAttempts; attempt++ {
		attempts++
		err := uploadFile(ctx, gcs, bucket, object, row.LocalPath, expectedMD5)
		if err == nil {
			return fileResult{ID: row.ID, Status: "completed", Attempts: attempts}
		}
		lastErr = err
		log.Warn("upload failed", "transfer_id", transferID, "file_id", row.FileID,
			"attempt", attempts, "err", err)
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			break
		}
		if attempt < MaxAttempts {
			jitter := time.Duration(rand.Int63n(int64(BackoffBase)))
			select {
			case <-time.After(BackoffBase*(1<<(attempt-1)) + jitter):
			case <-ctx.Done():
				return fileResult{ID: row.ID, Status: "failed",
					Error: ctx.Err().Error(), Attempts: attempts}
			}
		}
	}
	return fileResult{ID: row.ID, Status: "failed", Error: lastErr.Error(), Attempts: attempts}
}

// flusherLoop is the single writer; flushes on size or FLUSH_AFTER since the
// first buffered row, whichever first. Crash before flush loses up to
// FlushEvery rows / FlushAfter of work — those files stay 'pending' and
// re-upload on restart, where the HEAD-and-verify idempotency turns them
// into a no-op.
func flusherLoop(ctx context.Context, pool *pgxpool.Pool, results <-chan fileResult) error {
	var buf []fileResult
	var timer *time.Timer
	var timerC <-chan time.Time

	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		if err := flushBatch(ctx, pool, buf); err != nil {
			return err
		}
		buf = buf[:0]
		if timer != nil {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer = nil
			timerC = nil
		}
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case r, ok := <-results:
			if !ok {
				return flush()
			}
			if len(buf) == 0 {
				timer = time.NewTimer(FlushAfter)
				timerC = timer.C
			}
			buf = append(buf, r)
			if len(buf) >= FlushEvery {
				if err := flush(); err != nil {
					return err
				}
			}
		case <-timerC:
			if err := flush(); err != nil {
				return err
			}
		}
	}
}

// flushBatch writes a batch of terminal results via UPDATE FROM unnest(...).
// error is encoded as text[] and NULLed in SQL for completed rows, dodging
// the pgtype.Array[pgtype.Text] boilerplate for nullable text arrays.
// file_dest_url is owned by the external fan-out and never written here.
func flushBatch(ctx context.Context, pool *pgxpool.Pool, buf []fileResult) error {
	ids := make([]int64, len(buf))
	statuses := make([]string, len(buf))
	errs := make([]string, len(buf))
	attempts := make([]int32, len(buf))
	for i, r := range buf {
		ids[i] = r.ID
		statuses[i] = r.Status
		errs[i] = r.Error
		attempts[i] = r.Attempts
	}
	_, err := pool.Exec(ctx, `
        UPDATE transfer_file SET
            status       = u.status,
            error        = CASE WHEN u.status = 'completed' THEN NULL ELSE u.error END,
            attempts     = u.attempts,
            when_updated = now()
          FROM unnest($1::bigint[], $2::text[], $3::text[], $4::int[])
            AS u(id, status, error, attempts)
         WHERE transfer_file.transfer_file_id = u.id
    `, ids, statuses, errs, attempts)
	return err
}

// datasetManifest is required for every IDC transfer -- it identifies the
// submission -- so its absence is a setup failure rather than an empty case.
const datasetManifest = "dataset_manifest.csv"

// loadBaseGCSURL reads the package anchor that every relative path hangs off.
// Posda owns this value; the daemon only ever reads it.
func loadBaseGCSURL(ctx context.Context, pool *pgxpool.Pool, transferID int64) (string, error) {
	var base *string
	err := pool.QueryRow(ctx, `
        SELECT base_gcs_url FROM transfer_idc
         WHERE dataset_release_transfer_id = $1
    `, transferID).Scan(&base)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("transfer=%d: no transfer_idc row", transferID)
	}
	if err != nil {
		return "", err
	}
	if base == nil || strings.TrimSpace(*base) == "" {
		return "", fmt.Errorf("transfer=%d: transfer_idc.base_gcs_url is empty", transferID)
	}
	// Curator-editable free text in the Manage modal, so tolerate a trailing
	// slash instead of producing ".../v1//imaging/<md5>".
	anchor := strings.TrimRight(strings.TrimSpace(*base), "/")
	if !strings.HasPrefix(anchor, "gs://") {
		return "", fmt.Errorf("transfer=%d: base_gcs_url %q is not a gs:// URL", transferID, anchor)
	}
	return anchor, nil
}

// uploadManifests uploads whichever of IDC's three manifests this transfer
// carries, to fixed names at the package root. Which ones exist varies: Posda
// derives what is required from the transfer's content -- imaging when it
// carries DICOM, clinical when it carries Clinical Data files -- so absence is
// meaningful and not an error, except for the dataset manifest.
//
// Destinations come from the anchor directly. The previous version recovered
// the folder by stripping after the last slash of a sample file URL, which
// worked only while every object sat flat at <base>/<md5>; with files now under
// imaging/ and clinical/ it would have computed <base>/imaging and written the
// manifests one level too deep.
func uploadManifests(ctx context.Context, pool *pgxpool.Pool, gcs *storage.Client, transferID int64, baseURL string) ([]string, error) {
	rows, err := pool.Query(ctx, `
        SELECT DISTINCT ON (m.object_name)
               m.object_name,
               fsr.root_path || '/' || fl.rel_path
          FROM transfer_idc ti
          CROSS JOIN LATERAL (VALUES
                ('dataset_manifest.csv',  ti.dataset_manifest_file_id),
                ('imaging_manifest.csv',  ti.imaging_manifest_file_id),
                ('clinical_manifest.csv', ti.clinical_manifest_file_id)
             ) AS m(object_name, file_id)
          -- inner joins drop the manifests this transfer does not carry
          JOIN file_location      fl  ON fl.file_id = m.file_id
          JOIN file_storage_root  fsr ON fsr.file_storage_root_id = fl.file_storage_root_id
         WHERE ti.dataset_release_transfer_id = $1
         -- file_location has no unique constraint and does carry exact
         -- duplicate rows, so take one path per manifest. Any is equivalent:
         -- every duplicate observed resolves to the same root and rel_path.
         ORDER BY m.object_name
    `, transferID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type manifest struct{ object, localPath string }
	var found []manifest
	haveDataset := false
	for rows.Next() {
		var m manifest
		if err := rows.Scan(&m.object, &m.localPath); err != nil {
			return nil, err
		}
		if m.object == datasetManifest {
			haveDataset = true
		}
		found = append(found, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if !haveDataset {
		return nil, fmt.Errorf("transfer=%d: transfer_idc.%s missing "+
			"or its file has no location — external setup did not run",
			transferID, "dataset_manifest_file_id")
	}

	uploaded := make([]string, 0, len(found))
	for _, m := range found {
		url := baseURL + "/" + m.object
		bucket, object, err := parseGSURL(url)
		if err != nil {
			return nil, err
		}
		digest, err := md5OfFile(m.localPath)
		if err != nil {
			return nil, fmt.Errorf("compute md5 for %s: %w", m.object, err)
		}
		if err := uploadFile(ctx, gcs, bucket, object, m.localPath, digest); err != nil {
			return nil, fmt.Errorf("upload %s: %w", m.object, err)
		}
		uploaded = append(uploaded, url)
	}
	return uploaded, nil
}
