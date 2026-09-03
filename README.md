# idc-transfer-daemon

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
3. **Streams** file rows out of `transfer_file` (populated by Posda in the same
   transaction as the queue flip) in keyset-paginated batches, joining back to
   `file_location` to recover each file's local path and MD5.
4. **Uploads** files in parallel. `transfer_file.file_dest_url` is stored
   *relative* to the package anchor — `imaging/<md5>` for DICOM,
   `clinical/<name>` otherwise — and the daemon joins it to
   `transfer_idc.base_gcs_url`, read once per transfer. HEAD-then-verify-MD5
   makes re-runs idempotent.
5. **Records** per-file `completed` / `failed` status with retry/attempt counters,
   flushed in batches.
6. **Uploads the manifests** — IDC takes up to three (`dataset_manifest.csv`,
   `imaging_manifest.csv`, `clinical_manifest.csv`) at fixed names directly
   under the anchor. Which exist varies per transfer: Posda derives what is
   required from the content, so the daemon uploads whichever are present. Their
   URLs are not recorded — the names are fixed, so they are derivable. Then it
   marks the transfer `'success'` (or `'failed'` if any file is still unresolved
   after retries).

Concurrency is goroutines + buffered channels — one transfer at a time
(`TRANSFER_PARALLELISM = 1`), up to 200 upload workers within it. No external
queue; Postgres is the source of truth.

**The boundary: Posda decides, the daemon transports.** Posda owns what ships,
which files, manifest generation, the bucket path scheme and every gate.
`transfer_idc.base_gcs_url` in particular is written by Posda and only ever
*read* here — it is the package anchor every relative path hangs off, so a
relocation is a one-field edit on Posda's side. The daemon moves bytes, records
per-file progress, and reports terminal status.

## Layout

- `src/` — the Go daemon (`main.go`, `daemon.go`, `db.go`, `gcs.go`,
  `transfer.go`) plus its `Dockerfile` and `Makefile`.
- `full_schema.sql` — snapshot of the upstream Posda schema for reference
  (gitignored; obtain locally).

There is no `sql/` directory. It held a `transfer_idc_file` table, a duplicate
copy of the LISTEN/NOTIFY trigger, an external fan-out query and test fixtures —
all removed once Posda took ownership. Posda's
`database/migrations/posda_files/0047_add_dataset_module_tables.sql` is the
source of truth for the schema and the trigger, and Posda fans out
`transfer_file` rows itself at queue time.
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
