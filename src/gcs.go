package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
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
	if kf := os.Getenv("GCS_KEY_FILE"); kf != "" {
		opts = append(opts, option.WithCredentialsFile(kf))
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

	w := obj.NewWriter(ctx)
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
