.PHONY: run

PGUSER := postgres
PGPASSWORD := example
PGHOST := oneposda-db-1
PGPORT := 5432
PGDATABASE := posda_files
GELF_ADDR=tcia-graylog-1.ad.uams.edu:12201


default: build

run: .last_built
	docker run --rm --network oneposda_default \
	-e PGHOST=${PGHOST} \
	-e PGPORT=${PGPORT} \
	-e PGUSER=${PGUSER} \
	-e PGPASSWORD=${PGPASSWORD} \
	-e PGDATABASE=${PGDATABASE} \
	-e GCS_KEY_JSON="$$(cat sa-key.json)" \
	-e GELF_ADDR=${GELF_ADDR} \
	-e GELF_TAG=test-tag \
	--mount type=volume,source=oneposda_posda_cache_alpine,target=/home/posda/cache \
	tcia/transfer-daemon:go


build: .last_built


.last_built: *.go
	docker build -t tcia/transfer-daemon:go .
	touch .last_built
