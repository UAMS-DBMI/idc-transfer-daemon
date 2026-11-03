package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
)

// The handshake with IDC's ETL lives in the version folder, next to the
// manifests. Status values are exact literals: IDC string-compares them.
const (
	statusObject = "status.json"
	lockObject   = "status.lock"

	statusDraft      = "Draft"
	statusReady      = "Ready for processing"
	statusProcessing = "Processing"
	statusProcessed  = "Processed"
	statusErrored    = "Errored"

	lockMaxRetries = 5
)

var (
	errLockBusy      = errors.New("version lock is held by another worker")
	errLockNotHeld   = errors.New("version lock is not held by this worker")
	errVersionClosed = errors.New("version folder cannot be written")
)

type versionFolder struct {
	bucket *storage.BucketHandle
	prefix string // "<slug>/v<n>"
}

func openVersionFolder(gcs *storage.Client, baseURL string) (versionFolder, error) {
	bucket, prefix, err := parseGSURL(baseURL)
	if err != nil {
		return versionFolder{}, err
	}
	return versionFolder{bucket: gcs.Bucket(bucket), prefix: prefix}, nil
}

func (v versionFolder) object(name string) *storage.ObjectHandle {
	return v.bucket.Object(v.prefix + "/" + name)
}

func lockContent(workerID string) string {
	return "Locked by " + workerID
}

// acquireLock is an atomic create-if-absent, retried with exponential backoff.
// It is not reentrant: a lock this worker already holds is still busy.
func acquireLock(ctx context.Context, v versionFolder, workerID string, backoff time.Duration) error {
	for attempt := 0; ; attempt++ {
		w := v.object(lockObject).If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
		w.ContentType = "text/plain"
		_, err := io.WriteString(w, lockContent(workerID))
		if closeErr := w.Close(); err == nil {
			err = closeErr
		}
		if err == nil {
			return nil
		}
		if !isPreconditionFailed(err) {
			return fmt.Errorf("acquire lock: %w", err)
		}
		if attempt == lockMaxRetries {
			return errLockBusy
		}
		select {
		case <-time.After(backoff << attempt):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// releaseLock refuses to delete a lock this worker does not hold. The delete is
// conditioned on the generation that was read, so a lock replaced between the
// read and the delete survives.
func releaseLock(ctx context.Context, v versionFolder, workerID string) error {
	holder, generation, err := readLock(ctx, v)
	if err != nil {
		return err
	}
	if holder != lockContent(workerID) {
		return errLockNotHeld
	}
	err = v.object(lockObject).If(storage.Conditions{GenerationMatch: generation}).Delete(ctx)
	if isPreconditionFailed(err) {
		return errLockNotHeld
	}
	return err
}

func readLock(ctx context.Context, v versionFolder) (holder string, generation int64, err error) {
	r, err := v.object(lockObject).NewReader(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return "", 0, errLockNotHeld
	}
	if err != nil {
		return "", 0, fmt.Errorf("read lock: %w", err)
	}
	defer r.Close()
	content, err := io.ReadAll(r)
	if err != nil {
		return "", 0, fmt.Errorf("read lock: %w", err)
	}
	return string(content), r.Attrs.Generation, nil
}

func isPreconditionFailed(err error) bool {
	var apiErr *googleapi.Error
	return errors.As(err, &apiErr) && apiErr.Code == http.StatusPreconditionFailed
}

// readStatus returns "" when the folder has no status file yet.
func readStatus(ctx context.Context, v versionFolder) (string, error) {
	r, err := v.object(statusObject).NewReader(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read status: %w", err)
	}
	defer r.Close()
	var doc struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(r).Decode(&doc); err != nil {
		return "", fmt.Errorf("parse status: %w", err)
	}
	return doc.Status, nil
}

func writeStatus(ctx context.Context, v versionFolder, status string) error {
	w := v.object(statusObject).NewWriter(ctx)
	w.ContentType = "application/json"
	if err := json.NewEncoder(w).Encode(map[string]string{"status": status}); err != nil {
		_ = w.Close()
		return fmt.Errorf("write status: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("write status: %w", err)
	}
	return nil
}

// statusAllowsWrite is the rule for overwriting a version in place: anything
// IDC hasn't ingested can change. Processed needs a new version, and Processing
// under a lock we hold means IDC stopped mid-run, which a person should look at.
// An unknown value is refused because the literals are a contract with IDC.
func statusAllowsWrite(status string) bool {
	switch status {
	case "", statusDraft, statusReady, statusErrored:
		return true
	default:
		return false
	}
}

// beginVersionWrite takes the version's lock and marks it Draft, so IDC ignores
// the folder until the upload finishes. The caller must release the lock.
//
// The worker id is per transfer, because the daemon holds nothing across a
// restart and a transfer left in_progress is picked up again. Finding its own
// lock is that recovery. The claim guarantees one owner per transfer, so this
// can't take a lock from a live holder, and without it a crash would hold the
// version forever: the lock has no lease.
func beginVersionWrite(ctx context.Context, v versionFolder, workerID string, backoff time.Duration) error {
	holder, _, err := readLock(ctx, v)
	switch {
	case err == nil && holder == lockContent(workerID):
		if err := releaseLock(ctx, v, workerID); err != nil {
			return err
		}
	case err != nil && !errors.Is(err, errLockNotHeld):
		return err
	}
	if err := acquireLock(ctx, v, workerID, backoff); err != nil {
		return err
	}

	status, err := readStatus(ctx, v)
	if err == nil && !statusAllowsWrite(status) {
		err = fmt.Errorf("%w: status is %q", errVersionClosed, status)
	}
	if err == nil {
		err = writeStatus(ctx, v, statusDraft)
	}
	if err != nil {
		if releaseErr := releaseLock(ctx, v, workerID); releaseErr != nil {
			return errors.Join(err, releaseErr)
		}
		return err
	}
	return nil
}
