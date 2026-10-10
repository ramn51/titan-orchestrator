#!/usr/bin/env bash
set -euo pipefail

MASTER_HOST="${1:-}"
WORKER_PORT="${2:-8080}"
CAPABILITY="${3:-GENERAL}"
PERMANENT="${4:-true}"
MASTER_PORT="${TITAN_PORT:-9090}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV="$ROOT/.venv"

if [[ -z "$MASTER_HOST" ]]; then
  echo "Usage: $0 <MASTER_INTERNAL_IP> [WORKER_PORT] [CAPABILITY] [PERMANENT]" >&2
  echo "Example: $0 10.10.0.2 8080 GENERAL true" >&2
  exit 2
fi

[[ -f "$ROOT/Worker.jar" ]] || { echo "[ERROR] Missing $ROOT/Worker.jar" >&2; exit 1; }
[[ -f "$ROOT/config.properties" ]] || { echo "[ERROR] Missing $ROOT/config.properties" >&2; exit 1; }
command -v java >/dev/null || { echo "[ERROR] Java 17+ is required." >&2; exit 1; }
command -v python3 >/dev/null || { echo "[ERROR] Python 3 is required." >&2; exit 1; }

cd "$ROOT"
if [[ ! -x "$VENV/bin/python" ]]; then
  echo "[SETUP] Creating worker virtual environment..."
  python3 -m venv "$VENV"
fi
if [[ ! -f "$VENV/.titan-sdk-ready" ]]; then
  echo "[SETUP] Installing Titan SDK..."
  "$VENV/bin/python" -m pip install --quiet -e "$ROOT"
  touch "$VENV/.titan-sdk-ready"
fi

export PATH="$VENV/bin:$PATH"
export TITAN_HOST="$MASTER_HOST"
export TITAN_PORT="$MASTER_PORT"

echo "[START] Titan Worker"
echo "        Master:    $MASTER_HOST:$MASTER_PORT"
echo "        Worker:    0.0.0.0:$WORKER_PORT"
echo "        Capability: $CAPABILITY"
echo "        Permanent:  $PERMANENT"
exec java -jar "$ROOT/Worker.jar" "$WORKER_PORT" "$MASTER_HOST" "$MASTER_PORT" "$CAPABILITY" "$PERMANENT"
