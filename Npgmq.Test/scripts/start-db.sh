#!/bin/sh
set -e

PGMQ_VERSION=$1 

docker run -d -it --name npgmq_test_db -p 5432:5432 --rm quay.io/tembo/tembo-local

docker exec npgmq_test_db /bin/sh -c "psql -c \"CREATE DATABASE npgmq_test;\""

if [ -z "$PGMQ_VERSION" ] || [ "$PGMQ_VERSION" = "latest" ]; then
  docker exec npgmq_test_db /bin/sh -c "trunk install pgmq"
else
  docker exec npgmq_test_db /bin/sh -c "trunk install pgmq --version=${PGMQ_VERSION}"
fi
docker exec npgmq_test_db /bin/sh -c "psql -d npgmq_test -c \"CREATE EXTENSION pgmq CASCADE;\""
