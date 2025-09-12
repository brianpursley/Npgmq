#!/bin/sh
set -e

PGMQ_VERSION=$1 

if [ -n "$PGMQ_VERSION" ] && [ "$PGMQ_VERSION" != "latest" ]; then
  TAG="v${PGMQ_VERSION}"
else
  TAG="latest"
fi

docker run -d --name npgmq_test_db -e POSTGRES_PASSWORD=postgres -p 5432:5432 ghcr.io/pgmq/pg17-pgmq:"${TAG}"

until docker exec npgmq_test_db /bin/sh -c "pg_isready"; do
  sleep 1
done

docker exec npgmq_test_db /bin/sh -c "psql -c \"CREATE DATABASE npgmq_test;\""

docker exec npgmq_test_db /bin/sh -c "psql -d npgmq_test -c \"CREATE EXTENSION pgmq CASCADE;\""
