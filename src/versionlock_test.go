package main

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"
)

// Run against an emulator, never a real bucket:
//
//	docker run -d -p 4443:4443 fsouza/fake-gcs-server -scheme http -public-host localhost:4443
//	STORAGE_EMULATOR_HOST=localhost:4443 go test ./...
func testFolder(t *testing.T) versionFolder {
	t.Helper()
	if os.Getenv("STORAGE_EMULATOR_HOST") == "" {
		t.Skip("STORAGE_EMULATOR_HOST not set")
	}
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	bucket := client.Bucket("versionlock-test")
	_ = bucket.Create(ctx, "test", nil) // already there after the first test
	prefix := strings.ReplaceAll(t.Name(), " ", "_") + "/v1"
	v := versionFolder{bucket: bucket, prefix: prefix}
	for _, name := range []string{lockObject, statusObject} {
		_ = v.object(name).Delete(ctx)
	}
	return v
}

const noBackoff = time.Millisecond

func TestSecondWorkerCannotAcquire(t *testing.T) {
	v := testFolder(t)
	ctx := context.Background()
	if err := acquireLock(ctx, v, "a", noBackoff); err != nil {
		t.Fatal(err)
	}
	if err := acquireLock(ctx, v, "b", noBackoff); !errors.Is(err, errLockBusy) {
		t.Fatalf("got %v, want errLockBusy", err)
	}
}

func TestNonOwnerCannotRelease(t *testing.T) {
	v := testFolder(t)
	ctx := context.Background()
	if err := acquireLock(ctx, v, "a", noBackoff); err != nil {
		t.Fatal(err)
	}
	if err := releaseLock(ctx, v, "b"); !errors.Is(err, errLockNotHeld) {
		t.Fatalf("got %v, want errLockNotHeld", err)
	}
	if holder, _, err := readLock(ctx, v); err != nil || holder != lockContent("a") {
		t.Fatalf("lock changed: %q, %v", holder, err)
	}
}

func TestAcquireAfterRelease(t *testing.T) {
	v := testFolder(t)
	ctx := context.Background()
	if err := acquireLock(ctx, v, "a", noBackoff); err != nil {
		t.Fatal(err)
	}
	if err := releaseLock(ctx, v, "a"); err != nil {
		t.Fatal(err)
	}
	if err := acquireLock(ctx, v, "b", noBackoff); err != nil {
		t.Fatal(err)
	}
}

func TestLockIsNotReentrant(t *testing.T) {
	v := testFolder(t)
	ctx := context.Background()
	if err := acquireLock(ctx, v, "a", noBackoff); err != nil {
		t.Fatal(err)
	}
	if err := acquireLock(ctx, v, "a", noBackoff); !errors.Is(err, errLockBusy) {
		t.Fatalf("got %v, want errLockBusy", err)
	}
}

func TestReleaseAfterForcedTakeoverFails(t *testing.T) {
	v := testFolder(t)
	ctx := context.Background()
	if err := acquireLock(ctx, v, "a", noBackoff); err != nil {
		t.Fatal(err)
	}
	if err := v.object(lockObject).Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if err := acquireLock(ctx, v, "c", noBackoff); err != nil {
		t.Fatal(err)
	}
	if err := releaseLock(ctx, v, "a"); !errors.Is(err, errLockNotHeld) {
		t.Fatalf("got %v, want errLockNotHeld", err)
	}
	if holder, _, err := readLock(ctx, v); err != nil || holder != lockContent("c") {
		t.Fatalf("new holder's lock changed: %q, %v", holder, err)
	}
}

func TestBeginVersionWrite(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name, status string
		allowed      bool
	}{
		{"first push", "", true},
		{"draft", statusDraft, true},
		{"not yet picked up", statusReady, true},
		{"errored", statusErrored, true},
		{"processing", statusProcessing, false},
		{"processed", statusProcessed, false},
		{"unknown literal", "ready to process", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := testFolder(t)
			if tc.status != "" {
				if err := writeStatus(ctx, v, tc.status); err != nil {
					t.Fatal(err)
				}
			}
			err := beginVersionWrite(ctx, v, "t1", noBackoff)
			got, readErr := readStatus(ctx, v)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if tc.allowed {
				if err != nil {
					t.Fatal(err)
				}
				if got != statusDraft {
					t.Fatalf("status %q, want Draft", got)
				}
				return
			}
			if !errors.Is(err, errVersionClosed) {
				t.Fatalf("got %v, want errVersionClosed", err)
			}
			if got != tc.status {
				t.Fatalf("status changed to %q", got)
			}
			if _, _, err := readLock(ctx, v); !errors.Is(err, errLockNotHeld) {
				t.Fatalf("refusal left the lock behind: %v", err)
			}
		})
	}
}

func TestBeginVersionWriteResumesOwnLock(t *testing.T) {
	v := testFolder(t)
	ctx := context.Background()
	if err := acquireLock(ctx, v, "t1", noBackoff); err != nil {
		t.Fatal(err)
	}
	if err := beginVersionWrite(ctx, v, "t1", noBackoff); err != nil {
		t.Fatal(err)
	}
}

func TestBeginVersionWriteWaitsOnAnotherHolder(t *testing.T) {
	v := testFolder(t)
	ctx := context.Background()
	if err := acquireLock(ctx, v, "idc", noBackoff); err != nil {
		t.Fatal(err)
	}
	if err := beginVersionWrite(ctx, v, "t1", noBackoff); !errors.Is(err, errLockBusy) {
		t.Fatalf("got %v, want errLockBusy", err)
	}
}
