#!/bin/sh
set -eu

CASSANDRA_HOST="${CASSANDRA_HOST:-cassandra}"
CASSANDRA_PORT="${CASSANDRA_PORT:-9042}"

printf '%s\n' "Waiting for Cassandra at ${CASSANDRA_HOST}:${CASSANDRA_PORT}..."

until cqlsh "${CASSANDRA_HOST}" "${CASSANDRA_PORT}" -e "DESCRIBE KEYSPACES" >/dev/null 2>&1; do
  sleep 5
done

printf '%s\n' "Applying Cassandra init from /init/init.cassandra.cql..."
cqlsh "${CASSANDRA_HOST}" "${CASSANDRA_PORT}" -f /init/init.cassandra.cql
printf '%s\n' "Cassandra init finished."

