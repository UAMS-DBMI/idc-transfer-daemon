-- Transfer Preparation — external fan-out.
--
-- Run by the queuing process BEFORE flipping dataset_release_transfer.transfer_status
-- to 'queued'. Materializes one transfer_idc_file row per file in the transfer's
-- recordset, with the destination GCS URL computed as <base_path>/<md5>.
--
-- The daemon does not insert into transfer_idc_file — it only reads these rows.
--
-- Parameters:
--   $1 = dataset_release_transfer_id  (integer)
--   $2 = base_path                    (text, e.g. 'gs://posda_submit/some-collection/v1/t3')
--
-- Object names are content-addressed: two transfers shipping the same file_id
-- with the same digest compute the same URL, so the daemon's idempotency check
-- (HEAD by name, verify MD5) makes the upload a no-op. ON CONFLICT keeps the
-- fan-out itself re-runnable.

INSERT INTO public.transfer_idc_file
    (dataset_release_transfer_id, file_id, gcs_url)
SELECT
    $1,
    rrf.file_id,
    $2 || '/' || f.digest
FROM transfer_recordset      tr
JOIN recordset_release_file  rrf ON rrf.recordset_release_id = tr.recordset_release_id
JOIN file                    f   ON f.file_id                = rrf.file_id
WHERE tr.dataset_release_transfer_id = $1
ON CONFLICT (dataset_release_transfer_id, file_id) DO NOTHING;
