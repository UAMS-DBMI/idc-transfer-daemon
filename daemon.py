import asyncio
import base64
import logging
import os
import signal
import time
from concurrent.futures import ThreadPoolExecutor

import asyncpg
import requests
from google.cloud import storage

DSN          = os.environ["TRANSFER_DAEMON_DSN"]
CONCURRENCY  = 200          # per-transfer worker count
MAX_ATTEMPTS = 3            # inline upload retries per file before giving up
BACKOFF_BASE = 0.5          # seconds; exponential backoff between retries
FLUSH_EVERY  = 500          # batch this many updates before flushing
FLUSH_AFTER  = 2.0          # ...or this many seconds after the first buffered row, whichever first
POOL_MIN     = 2
POOL_MAX     = 6
TRANSFER_PARALLELISM = 1    # at most one transfer in flight at a time
RECONCILE_INTERVAL   = 300  # safety-net re-scan for missed NOTIFYs (seconds)
NOTIFY_CHANNEL = "idc_transfer_channel"

SERVICE_ACCOUNT_JSON = "sa-key.json"
GCP_PROJECT          = "tcia-data-transfers"

log = logging.getLogger("idc_transfer_daemon")
_FLUSHER_DONE = object()  # sentinel for shutting down the flusher


def make_gcs_client() -> storage.Client:
    # Size the HTTP connection pool to CONCURRENCY; the urllib3 default (~10)
    # would serialise the workers no matter how many coroutines we run.
    client = storage.Client.from_service_account_json(
        SERVICE_ACCOUNT_JSON, project=GCP_PROJECT
    )
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=CONCURRENCY, pool_maxsize=CONCURRENCY
    )
    client._http.mount("https://", adapter)  # _http is the AuthorizedSession (a requests.Session)
    client._http.mount("http://", adapter)
    return client


def parse_gs_url(url: str) -> tuple[str, str]:
    # "gs://bucket/path/to/object" -> ("bucket", "path/to/object")
    if not url.startswith("gs://"):
        raise ValueError(f"not a gs:// URL: {url!r}")
    bucket, _, name = url[5:].partition("/")
    if not bucket or not name:
        raise ValueError(f"malformed gs:// URL: {url!r}")
    return bucket, name


def gcs_md5_hex(blob: storage.Blob) -> str | None:
    # GCS exposes md5 as base64; convert to hex to compare with file.digest.
    # Composite objects have no md5 — return None so the caller re-uploads.
    if blob.md5_hash is None:
        return None
    return base64.b64decode(blob.md5_hash).hex()


async def claim_transfer(pool, transfer_id):
    """Atomic claim. Returns True iff this daemon now owns the transfer."""
    async with pool.acquire() as conn, conn.transaction():
        row = await conn.fetchrow(
            """
            UPDATE dataset_release_transfer
               SET transfer_status = 'in_progress', when_updated = now(),
                   who_updated = 'idc_transfer_daemon'
             WHERE dataset_release_transfer_id = $1
               AND transfer_status IN ('queued', 'in_progress')
            RETURNING dataset_release_transfer_id
            """,
            transfer_id,
        )
        if row is None:
            return False

        # Fan-out into transfer_idc_file happened externally before this transfer
        # was queued; the daemon does not insert into transfer_idc_file.

        # On re-queue, give previously failed files another pass.
        await conn.execute(
            """
            UPDATE transfer_idc_file
               SET status='pending', attempts=0, error=NULL, updated_at=now()
             WHERE dataset_release_transfer_id = $1 AND status = 'failed'
            """,
            transfer_id,
        )
    return True


async def file_producer(pool, transfer_id, queue):
    # Keyset pagination in short autocommit queries — no hours-long open
    # transaction pinning the xmin horizon while 500k rows churn.
    # local_path and md5 are re-joined here rather than stored on transfer_idc_file.
    last_id = 0
    while True:
        rows = await pool.fetch(
            """
            SELECT tif.transfer_idc_file_id              AS id,
                   tif.file_id                           AS file_id,
                   tif.gcs_url                           AS gcs_url,
                   fsr.root_path || '/' || fl.rel_path   AS local_path,
                   f.digest                              AS md5,
                   tif.attempts                          AS attempts
              FROM transfer_idc_file  tif
              JOIN file               f   ON f.file_id   = tif.file_id
              JOIN file_location      fl  ON fl.file_id  = tif.file_id
              JOIN file_storage_root  fsr ON fsr.file_storage_root_id = fl.file_storage_root_id
             WHERE tif.dataset_release_transfer_id = $1
               AND tif.status = 'pending'
               AND tif.transfer_idc_file_id > $2
             ORDER BY tif.transfer_idc_file_id
             LIMIT 1000
            """,
            transfer_id, last_id,
        )
        if not rows:
            break
        for row in rows:
            await queue.put(row)
        last_id = rows[-1]["id"]
    for _ in range(CONCURRENCY):
        await queue.put(None)  # poison pills to shut down consumers


async def upload_one(transfer_id, gcs_client, row):
    """Upload one file with inline retry+backoff. Returns a *terminal* result tuple.

    The producer streams each row exactly once, so a worker must drive its file
    all the way to 'completed' or 'failed' here — there is no second pass.
    """
    bucket_name, object_name = parse_gs_url(row["gcs_url"])
    bucket   = gcs_client.bucket(bucket_name)
    attempts = row["attempts"]
    last_err = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        attempts += 1
        try:
            existing = await asyncio.to_thread(bucket.get_blob, object_name)  # one HEAD
            if existing is not None and gcs_md5_hex(existing) == row["md5"]:
                return (row["id"], "completed", None, attempts)

            blob = bucket.blob(object_name)
            await asyncio.to_thread(
                blob.upload_from_filename, row["local_path"], checksum="md5"
            )  # checksum="md5" → GCS rejects a corrupted round trip
            return (row["id"], "completed", None, attempts)
        except Exception as exc:
            last_err = exc
            log.warning(
                "upload failed transfer=%s file_id=%s attempt=%s err=%s",
                transfer_id, row["file_id"], attempts, exc,
            )
            if attempt < MAX_ATTEMPTS:
                await asyncio.sleep(BACKOFF_BASE * 2 ** (attempt - 1))

    return (row["id"], "failed", str(last_err), attempts)


async def file_consumer(transfer_id, gcs_client, queue, results):
    while True:
        row = await queue.get()
        if row is None:
            return
        await results.put(await upload_one(transfer_id, gcs_client, row))


async def flusher(pool, results):
    """Single writer. Flushes on size OR FLUSH_AFTER since the first buffered row."""
    buffer = []
    first_at = None
    while True:
        timeout = None if first_at is None else max(0.0, FLUSH_AFTER - (time.monotonic() - first_at))
        try:
            item = await asyncio.wait_for(results.get(), timeout=timeout)
        except asyncio.TimeoutError:
            item = None  # time-based flush trigger

        if item is _FLUSHER_DONE:
            break
        if item is not None:
            if not buffer:
                first_at = time.monotonic()
            buffer.append(item)

        time_up = first_at is not None and (time.monotonic() - first_at) >= FLUSH_AFTER
        if buffer and (len(buffer) >= FLUSH_EVERY or item is None or time_up):
            await flush(pool, buffer)
            buffer.clear()
            first_at = None

    if buffer:
        await flush(pool, buffer)


async def flush(pool, buffer):
    # gcs_url is owned by the external fan-out and never written here.
    await pool.execute(
        """
        UPDATE transfer_idc_file SET
            status     = u.status,
            error      = u.error,
            attempts   = u.attempts,
            updated_at = now()
        FROM unnest($1::int[], $2::text[], $3::text[], $4::int[])
            AS u(id, status, error, attempts)
        WHERE transfer_idc_file.transfer_idc_file_id = u.id
        """,
        [r[0] for r in buffer],
        [r[1] for r in buffer],
        [r[2] for r in buffer],
        [r[3] for r in buffer],
    )


async def upload_manifest(pool, gcs_client, transfer_id):
    """Upload the externally-prepared manifest from Posda to GCS.

    Destination is '<base_path>/manifest.csv', where <base_path> is recovered
    by stripping the trailing /<md5> from any transfer_idc_file row's gcs_url.
    Returns the manifest's gs:// URL.
    """
    manifest = await pool.fetchrow(
        """
        SELECT fsr.root_path || '/' || fl.rel_path AS local_path
          FROM transfer_idc       ti
          JOIN file_location      fl  ON fl.file_id = ti.dataset_manifest_file_id
          JOIN file_storage_root  fsr ON fsr.file_storage_root_id = fl.file_storage_root_id
         WHERE ti.dataset_release_transfer_id = $1
        """,
        transfer_id,
    )
    if manifest is None:
        raise RuntimeError(
            f"transfer={transfer_id}: transfer_idc.dataset_manifest_file_id missing "
            "or its file has no location — external setup did not run"
        )

    sample = await pool.fetchval(
        "SELECT gcs_url FROM transfer_idc_file "
        "WHERE dataset_release_transfer_id = $1 LIMIT 1",
        transfer_id,
    )
    base_path    = sample.rsplit("/", 1)[0]  # strip trailing /<md5>
    manifest_url = f"{base_path}/manifest.csv"

    bucket_name, object_name = parse_gs_url(manifest_url)
    blob = gcs_client.bucket(bucket_name).blob(object_name)
    await asyncio.to_thread(
        blob.upload_from_filename,
        manifest["local_path"],
        checksum="md5",
    )
    return manifest_url


async def process_transfer(pool, gcs_client, transfer_id):
    if not await claim_transfer(pool, transfer_id):
        log.info("transfer=%s skipped (not claimable)", transfer_id)
        return

    log.info("transfer=%s starting", transfer_id)

    queue   = asyncio.Queue(maxsize=CONCURRENCY * 2)
    results = asyncio.Queue(maxsize=FLUSH_EVERY * 4)  # bounded: backpressure if the DB stalls

    flusher_task = asyncio.create_task(flusher(pool, results))
    consumers = [
        asyncio.create_task(file_consumer(transfer_id, gcs_client, queue, results))
        for _ in range(CONCURRENCY)
    ]
    await file_producer(pool, transfer_id, queue)
    await asyncio.gather(*consumers)
    await results.put(_FLUSHER_DONE)
    await flusher_task

    counts = await pool.fetchrow(
        """
        SELECT COUNT(*) FILTER (WHERE status = 'completed')  AS ok,
               COUNT(*) FILTER (WHERE status <> 'completed') AS not_ok
          FROM transfer_idc_file
         WHERE dataset_release_transfer_id = $1
        """,
        transfer_id,
    )
    log.info(
        "transfer=%s files completed=%s incomplete=%s",
        transfer_id, counts["ok"], counts["not_ok"],
    )

    if counts["not_ok"] > 0 or counts["ok"] == 0:
        # Any non-'completed' row blocks submission. ok==0 catches the empty case
        # (external fan-out didn't run, or recordset was empty) — refuse to mark success.
        await pool.execute(
            "UPDATE dataset_release_transfer SET transfer_status='failed', "
            "when_updated=now(), who_updated='idc_transfer_daemon' "
            "WHERE dataset_release_transfer_id=$1",
            transfer_id,
        )
        log.warning("transfer=%s marked failed", transfer_id)
        return

    manifest_url = await upload_manifest(pool, gcs_client, transfer_id)
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute(
            "UPDATE transfer_idc SET gcs_url=$1 WHERE dataset_release_transfer_id=$2",
            manifest_url, transfer_id,
        )
        await conn.execute(
            "UPDATE dataset_release_transfer SET transfer_status='success', "
            "when_updated=now(), who_updated='idc_transfer_daemon' "
            "WHERE dataset_release_transfer_id=$1",
            transfer_id,
        )
    log.info("transfer=%s succeeded manifest=%s", transfer_id, manifest_url)


async def recover_stale(db):
    """Find every transfer that is queued or already in progress.
    Accepts a pool or a connection (both expose .fetch)."""
    rows = await db.fetch(
        "SELECT dataset_release_transfer_id FROM dataset_release_transfer "
        "WHERE transfer_status IN ('queued', 'in_progress') "
        "ORDER BY dataset_release_transfer_id"
    )
    return [r["dataset_release_transfer_id"] for r in rows]


async def listen_loop(pending, stop):
    """Maintain a dedicated LISTEN connection, reconnecting on drop.
    NOTIFY is not durable, so re-scan for stale transfers on every (re)connect."""
    while not stop.is_set():
        conn = None
        try:
            conn = await asyncpg.connect(DSN)
            await conn.add_listener(
                NOTIFY_CHANNEL,
                lambda _c, _pid, _ch, payload: pending.put_nowait(int(payload)),
            )
            log.info("listening on %s", NOTIFY_CHANNEL)
            for tid in await recover_stale(conn):
                pending.put_nowait(tid)
            while not stop.is_set() and not conn.is_closed():
                await asyncio.sleep(1)
        except Exception:
            log.exception("listener connection lost; retrying in 5s")
            await asyncio.sleep(5)
        finally:
            if conn is not None:
                await conn.close()


async def reconcile_loop(pool, pending, stop):
    """Safety net for missed NOTIFYs and transfers stranded mid-flight."""
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=RECONCILE_INTERVAL)
            return  # stop requested
        except asyncio.TimeoutError:
            pass
        for tid in await recover_stale(pool):
            pending.put_nowait(tid)


async def daemon():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    loop = asyncio.get_running_loop()
    loop.set_default_executor(ThreadPoolExecutor(max_workers=CONCURRENCY))

    pool = await asyncpg.create_pool(DSN, min_size=POOL_MIN, max_size=POOL_MAX)
    gcs  = make_gcs_client()

    pending      = asyncio.Queue()
    transfer_sem = asyncio.Semaphore(TRANSFER_PARALLELISM)
    inflight     = set()      # dedupe: recovery + NOTIFY + reconcile may all enqueue one id
    stop         = asyncio.Event()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    listener   = asyncio.create_task(listen_loop(pending, stop))
    reconciler = asyncio.create_task(reconcile_loop(pool, pending, stop))
    tasks      = set()

    async def run_one(transfer_id):
        async with transfer_sem:
            try:
                await process_transfer(pool, gcs, transfer_id)
            except Exception:
                log.exception("transfer=%s crashed", transfer_id)
            finally:
                inflight.discard(transfer_id)

    while not stop.is_set():
        try:
            transfer_id = await asyncio.wait_for(pending.get(), timeout=1.0)
        except asyncio.TimeoutError:
            continue
        if transfer_id in inflight:
            continue  # already running; claim CAS would no-op anyway
        inflight.add(transfer_id)
        t = asyncio.create_task(run_one(transfer_id))
        tasks.add(t)
        t.add_done_callback(tasks.discard)

    log.info("shutdown requested; draining %d in-flight transfer(s)", len(tasks))
    listener.cancel()
    reconciler.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    await pool.close()


if __name__ == "__main__":
    asyncio.run(daemon())
