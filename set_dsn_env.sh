# Exports the PG* variables from docker_database.env so the daemon (and psql)
# pick them up directly; the daemon no longer takes a DSN.
set -a
. ./docker_database.env
set +a
export PGDATABASE=posda_files
