#!/bin/sh
set -e

cleanup() {
  "$(dirname "$0")"/stop-db.sh >/dev/null 2>&1
}

trap cleanup EXIT
"$(dirname "$0")"/start-db.sh "$@"

dotnet test "$(dirname "$0")"/../
cleanup
