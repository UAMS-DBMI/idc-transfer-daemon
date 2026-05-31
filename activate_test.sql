UPDATE dataset_release_transfer
	SET transfer_status = 'queued', when_updated = now(),
		who_updated = 'test_data_sql'
WHERE dataset_release_transfer_id = 8
;
