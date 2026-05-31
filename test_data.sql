-- Test data for the IDC Transfer Daemon.
--
-- Creates one dataset_release_transfer in 'draft' status, links it to a
-- populated recordset_release via transfer_recordset, creates the companion
-- transfer_idc row with a real dataset_manifest_file_id, and fans the
-- recordset out into transfer_idc_file with the daemon's pending URLs.
--
-- After this script runs you still need to flip the transfer to 'queued' to
-- fire the trigger and wake the daemon — the final UPDATE is commented out
-- so you can inspect the prepared rows first. The transfer_id is printed
-- in a RAISE NOTICE at the end.
--
-- Re-runnable: any prior transfer with the same transfer_name is deleted
-- first (CASCADE cleans transfer_idc and transfer_idc_file).
--
-- Data choices:
--   dataset_release_id = 3   (Quasar-collection-1, v1)
--   recordset_release_id = 7 (V1 — 715 files, all with file_location rows)
--   destination_id = 1       (Imaging Data Commons)
--   transfer_mode_id = 1     (single dataset)
--   base_path = gs://posda_submit/quasar-collection-1/v1/test
--   manifest  = first file_id in the recordset (re-used as a stand-in;
--              the daemon uploads it to <base_path>/manifest.csv on success)

BEGIN;

-- 1. Clean up any prior run of this script.
DELETE FROM public.dataset_release_transfer
 WHERE transfer_name = 'TEST: Quasar-collection-1 IDC daemon smoke test';

-- 2. Insert the transfer in 'draft', link the recordset, attach an IDC row
--    with a real manifest file_id, and fan out into transfer_idc_file —
--    all in one DO block so we can thread the generated id through.
DO $$
DECLARE
    v_transfer_id integer;
    v_manifest_file_id integer;
    v_base_path text := 'gs://posda_submit/quasar-collection-1/v1/test';
    v_file_count integer;
BEGIN
    INSERT INTO public.dataset_release_transfer (
        dataset_release_id,
        destination_id,
        transfer_name,
        transfer_mode_id,
        transfer_status,
        transfer_notes,
        when_created, who_created,
        when_updated, who_updated
    ) VALUES (
        3,  -- Quasar-collection-1, release 1
        1,  -- IDC
        'TEST: Quasar-collection-1 IDC daemon smoke test',
        1,  -- single dataset
        'draft',
        'Synthetic test transfer for the IDC transfer daemon.',
        now(), 'test_data_sql',
        now(), 'test_data_sql'
    )
    RETURNING dataset_release_transfer_id INTO v_transfer_id;

    INSERT INTO public.transfer_recordset (
        dataset_release_transfer_id,
        recordset_release_id
    ) VALUES (
        v_transfer_id,
        7  -- V1 — has 715 files
    );

    -- Pick any real file in the recordset to stand in as the dataset manifest.
    -- The daemon only needs it to resolve to a local path via file_location;
    -- for the smoke test it can be one of the files we're already uploading.
    SELECT rrf.file_id
      INTO v_manifest_file_id
      FROM public.recordset_release_file rrf
      JOIN public.file_location fl ON fl.file_id = rrf.file_id
     WHERE rrf.recordset_release_id = 7
     ORDER BY rrf.file_id
     LIMIT 1;

    IF v_manifest_file_id IS NULL THEN
        RAISE EXCEPTION 'no file with a file_location in recordset_release 7';
    END IF;

    INSERT INTO public.transfer_idc (
        dataset_release_transfer_id,
        dataset_manifest_file_id,
        published,
        public
    ) VALUES (
        v_transfer_id,
        v_manifest_file_id,
        false,
        false
    );

    -- 3. Fan-out: same query as transfer_preparation.sql, inlined so this
    --    script is self-contained.
    INSERT INTO public.transfer_idc_file
        (dataset_release_transfer_id, file_id, gcs_url)
    SELECT
        v_transfer_id,
        rrf.file_id,
        v_base_path || '/' || f.digest
    FROM public.transfer_recordset      tr
    JOIN public.recordset_release_file  rrf ON rrf.recordset_release_id = tr.recordset_release_id
    JOIN public.file                    f   ON f.file_id                = rrf.file_id
    WHERE tr.dataset_release_transfer_id = v_transfer_id
    ON CONFLICT (dataset_release_transfer_id, file_id) DO NOTHING;

    SELECT COUNT(*) INTO v_file_count
      FROM public.transfer_idc_file
     WHERE dataset_release_transfer_id = v_transfer_id;

    RAISE NOTICE '------------------------------------------------------------';
    RAISE NOTICE 'created dataset_release_transfer_id = %', v_transfer_id;
    RAISE NOTICE 'manifest file_id = %', v_manifest_file_id;
    RAISE NOTICE 'base_path = %', v_base_path;
    RAISE NOTICE 'transfer_idc_file rows = %', v_file_count;
    RAISE NOTICE 'status = draft (flip to ''queued'' to wake the daemon)';
    RAISE NOTICE '------------------------------------------------------------';
    RAISE NOTICE 'To activate:';
    RAISE NOTICE '  UPDATE dataset_release_transfer';
    RAISE NOTICE '     SET transfer_status = ''queued'', when_updated = now(),';
    RAISE NOTICE '         who_updated = ''test_data_sql''';
    RAISE NOTICE '   WHERE dataset_release_transfer_id = %;', v_transfer_id;
    RAISE NOTICE '------------------------------------------------------------';
END
$$;

COMMIT;

-- Uncomment to also flip the transfer to 'queued' in the same script,
-- which fires the AFTER UPDATE trigger and notifies the daemon:
--
-- UPDATE public.dataset_release_transfer
--    SET transfer_status = 'queued',
--        when_updated    = now(),
--        who_updated     = 'test_data_sql'
--  WHERE transfer_name   = 'TEST: Quasar-collection-1 IDC daemon smoke test';
