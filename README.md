# tcia-to-idc-pipeline

A Go daemon that ships file transfers from Posda (the TCIA submissions system) to
Google Cloud Storage buckets that NCI's Imaging Data Commons (IDC) ingests from.

## What it does

Posda's `dataset_release_transfer` table is the queue. Each row is a Transfer
Request — a request to upload one recordset (typically ~500k files, ~256 GB) to a
specific GCS destination and record a manifest of the resulting object URLs.

The daemon:

1. **Listens** for transfers flipped to `transfer_status = 'queued'` via Postgres
   `LISTEN/NOTIFY` on `idc_transfer_channel`, with a periodic reconciler as a
   safety net for missed notifications.
2. **Claims** a transfer (`'queued' → 'in_progress'`) in a single transaction so
   only one daemon ever owns it.
3. **Streams** file rows out of `transfer_idc_file` (pre-populated by an external
   fan-out step) in keyset-paginated batches, joining back to `file_location` to
   recover each file's local path and MD5.
4. **Uploads** files in parallel to their pre-computed, content-addressed GCS URLs
   (`<base_path>/<md5>`). HEAD-then-verify-MD5 makes re-runs idempotent.
5. **Records** per-file `completed` / `failed` status with retry/attempt counters,
   flushed in batches.
6. **Uploads the manifest file** and writes its URL into `transfer_idc.gcs_url`,
   then marks the transfer `'success'` (or `'failed'` if any file is still
   unresolved after retries).

Concurrency is goroutines + buffered channels — one transfer at a time
(`TRANSFER_PARALLELISM = 1`), up to 200 upload workers within it. No external
queue; Postgres is the source of truth.

## Layout

- `src/` — the Go daemon (`main.go`, `daemon.go`, `db.go`, `gcs.go`,
  `transfer.go`) plus its `Dockerfile` and `Makefile`.
- `sql/` — schema additions and test fixtures:
  - `schema_additions.sql` — the `transfer_idc_file` table, the
    `'in_progress'` status, and the LISTEN/NOTIFY trigger.
  - `transfer_preparation.sql` — the external fan-out query that materializes
    `transfer_idc_file` rows before a transfer is queued.
  - `test_data.sql`, `activate_test.sql` — fixtures for local end-to-end runs.
- `full_schema.sql` — snapshot of the upstream Posda schema for reference.
- `PLAN.md` — full architecture and design rationale; read this before changing
  the daemon's claim/recovery, batching, or status semantics.

## Configuration

Environment variables:

- `TRANSFER_DAEMON_DSN` — Postgres DSN for the Posda database (required).
- `GCS_KEY_FILE` — path to a GCS service-account key file (required at runtime;
  see `src/Makefile` for the Docker invocation used during development).
- `GELF_ADDR` — optional `host:port` for a GELF UDP log target (e.g.
  `graylog:12201`). When set, logs fan out to stderr *and* GELF; when unset,
  stderr only. A dial failure at startup falls back to stderr-only.
- `GELF_TAG` — optional value for the `_tag` field on every GELF record.
  Defaults to `idc-transfer-daemon`. Useful when several daemons share a
  Graylog instance and need distinct routing.

## Running locally

```sh
cd src
make build   # builds tcia/transfer-daemon:go
make run     # runs against a local oneposda Docker network
```
