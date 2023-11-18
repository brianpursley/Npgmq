#!/bin/sh
set -e

cleanup() {
  "$(dirname "$0")"/stop-db.sh
}

trap cleanup EXIT

"$(dirname "$0")"/start-db.sh "$@"

dotnet test ../
