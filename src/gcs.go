package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
	htransport "google.golang.org/api/transport/http"
)

func newGCSClient(ctx context.Context) (*storage.Client, error) {
	// storage.NewClient injects this scope when it builds its own transport;
	// we build ours, so we have to set it ourselves or the token comes back
	// with no scope and Google rejects it.
	opts := []option.ClientOption{option.WithScopes(storage.ScopeFullControl)}
	// The key is passed by value in GCS_KEY_JSON rather than by path, so the
	// container needs no secret volume. A malformed value fails NewTransport
	// below rather than being ignored.
	if kj := os.Getenv("GCS_KEY_JSON"); kj != "" {
		opts = append(opts, option.WithCredentialsJSON([]byte(kj)))
	} else if _, err := os.Stat("sa-key.json"); err == nil {
		opts = append(opts, option.WithCredentialsFile("sa-key.json"))
	}

	// Default http.Transport has MaxIdleConnsPerHost=2; with Concurrency
	// workers in flight that would force constant TCP+TLS reconnects.
	base := &http.Transport{
		MaxIdleConns:        Concurrency * 2,
		MaxIdleConnsPerHost: Concurrency * 2,
		IdleConnTimeout:     90 * time.Second,
	}
	rt, err := htransport.NewTransport(ctx, base, opts...)
	if err != nil {
		return nil, fmt.Errorf("build gcs transport: %w", err)
	}
	return storage.NewClient(ctx, option.WithHTTPClient(&http.Client{Transport: rt}))
}

// serviceAccountEmail pulls client_email out of a service-account key so the
// startup log can name the uploading identity without echoing the key itself.
// Returns "" when keyJSON is empty or unparseable; newGCSClient reports the
// latter as a hard error, so nothing is lost by staying quiet here.
func serviceAccountEmail(keyJSON string) string {
	if keyJSON == "" {
		return ""
	}
	var key struct {
		ClientEmail string `json:"client_email"`
	}
	if err := json.Unmarshal([]byte(keyJSON), &key); err != nil {
		return ""
	}
	return key.ClientEmail
}

func parseGSURL(url string) (bucket, name string, err error) {
	if !strings.HasPrefix(url, "gs://") {
		return "", "", fmt.Errorf("not a gs:// URL: %q", url)
	}
	rest := url[5:]
	i := strings.IndexByte(rest, '/')
	if i < 1 || i == len(rest)-1 {
		return "", "", fmt.Errorf("malformed gs:// URL: %q", url)
	}
	return rest[:i], rest[i+1:], nil
}

const (
	// chunkGranularity is the unit GCS resumable uploads work in; the client
	// rounds ChunkSize up to a multiple of it anyway.
	chunkGranularity = 256 * 1024
	// maxChunkSize bounds the per-writer buffer. With Concurrency writers in
	// flight this is also the worst-case upload memory: 200 x 4MiB = 800MiB.
	maxChunkSize = 4 * 1024 * 1024
)

// uploadChunkSize picks storage.Writer.ChunkSize for a file of the given size.
//
// Every Writer allocates a ChunkSize buffer up front regardless of the object
// size, and the library default is 16MiB. With 200 concurrent workers that is
// 3.2GiB of mostly empty buffers before a byte is sent, which OOM-killed the
// pod under a 2Gi limit. Sizing the buffer to the file (rounded up to the
// 256KiB granularity) lets the typical sub-megabyte DICOM file go out in one
// request with a right-sized buffer; the cap turns the rare large file into
// a resumable upload in maxChunkSize pieces rather than one huge buffer.
func uploadChunkSize(fileSize int64) int {
	if fileSize <= 0 {
		return chunkGranularity
	}
	if fileSize >= maxChunkSize {
		return maxChunkSize
	}
	rounded := (fileSize + chunkGranularity - 1) / chunkGranularity * chunkGranularity
	return int(rounded)
}

// uploadFile uploads localPath to gs://bucket/object, verifying GCS-side
// against expectedMD5 (raw 16-byte digest). Returns nil if a matching object
// already exists. The caller is responsible for retries.
func uploadFile(ctx context.Context, gcs *storage.Client, bucket, object, localPath string, expectedMD5 []byte) error {
	obj := gcs.Bucket(bucket).Object(object)

	if attrs, err := obj.Attrs(ctx); err == nil {
		if len(attrs.MD5) > 0 && bytes.Equal(attrs.MD5, expectedMD5) {
			return nil
		}
	} else if !errors.Is(err, storage.ErrObjectNotExist) {
		return fmt.Errorf("attrs: %w", err)
	}

	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open local: %w", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat local: %w", err)
	}

	w := obj.NewWriter(ctx)
	w.ChunkSize = uploadChunkSize(info.Size())
	w.MD5 = expectedMD5 // GCS rejects the upload server-side on mismatch.
	if _, err := io.Copy(w, f); err != nil {
		_ = w.Close()
		return fmt.Errorf("copy: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("finalize: %w", err)
	}
	return nil
}

// md5OfFile streams a file through md5 and returns the raw 16-byte digest.
// Used for the manifest, where we don't already hold the digest in the DB.
func md5OfFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

func decodeMD5Hex(s string) ([]byte, error) {
	return hex.DecodeString(s)
}
