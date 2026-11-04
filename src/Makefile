.PHONY: run build push

REGISTRY := registry.dbmi.cloud
IMAGE_NAME := idc-transfer-daemon
VERSION := 0.1.0
# The cluster is amd64; dev machines are arm64 Macs. Always build for the
# cluster, never the local platform.
PLATFORM := linux/amd64
IMAGE := ${REGISTRY}/${IMAGE_NAME}:${VERSION}

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
	${IMAGE}


build: .last_built


.last_built: *.go Dockerfile
	docker buildx build --platform ${PLATFORM} --load -t ${IMAGE} .
	touch .last_built


# Push the image built by `make build`. Builds first if the stamp is stale
# so the pushed tag always matches the current source.
push: .last_built
	docker push ${IMAGE}
