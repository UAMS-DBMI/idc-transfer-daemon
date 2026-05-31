.PHONY: run

PGUSER := postgres
PGPASSWORD := example
PGHOST := oneposda-db-1
PGPORT := 5432


default: build

run:
	docker run --rm --network oneposda_default \
	-e TRANSFER_DAEMON_DSN='postgresql://${PGUSER}:${PGPASSWORD}@${PGHOST}:${PGPORT}/posda_files' \
    -e GCS_KEY_FILE=/secrets/sa-key.json \
    -v /Users/quasar/projects/tcia-to-idc-pipeline/go-test/sa-key.json:/secrets/sa-key.json:ro \
	--mount type=volume,source=oneposda_posda_cache_alpine,target=/home/posda/cache \
    tcia/transfer-daemon:go


build: *.go
	docker build -t tcia/transfer-daemon:go .
