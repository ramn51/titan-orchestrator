#!/usr/bin/env sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec python3 "$ROOT/package_cloud.py" "$@"
