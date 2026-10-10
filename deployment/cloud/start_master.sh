#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PERM="$ROOT/perm_files"
VENV="$ROOT/.venv"
PID_DIR="$ROOT/.pids"

require_file() {
  [[ -f "$1" ]] || { echo "[ERROR] Missing $1" >&2; exit 1; }
}

stop_pid() {
  local name="$1"
  local file="$PID_DIR/$name.pid"
  [[ -f "$file" ]] || return 0
  local pid
  pid="$(cat "$file")"
  if [[ "$pid" =~ ^[0-9]+$ ]] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid"
    for _ in {1..20}; do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.25
    done
  fi
  rm -f "$file"
}

status() {
  local failed=0
  for name in store master dashboard; do
    local file="$PID_DIR/$name.pid"
    if [[ -f "$file" ]] && kill -0 "$(cat "$file")" 2>/dev/null; then
      echo "[OK] $name running (PID $(cat "$file"))"
    else
      echo "[DOWN] $name"
      failed=1
    fi
  done
  return "$failed"
}

case "${1:-start}" in
  stop)
    stop_pid dashboard
    stop_pid master
    stop_pid store
    echo "[OK] Titan Master bundle stopped."
    exit 0
    ;;
  status)
    status
    exit $?
    ;;
  start) ;;
  *) echo "Usage: $0 [start|stop|status]" >&2; exit 2 ;;
esac

require_file "$PERM/TitanStore.jar"
require_file "$PERM/titan-orchestrator-1.0-SNAPSHOT.jar"
require_file "$PERM/server_dashboard.py"
require_file "$ROOT/config.properties"
command -v java >/dev/null || { echo "[ERROR] Java 17+ is required." >&2; exit 1; }
command -v python3 >/dev/null || { echo "[ERROR] Python 3 is required." >&2; exit 1; }

mkdir -p "$PID_DIR" "$ROOT/uploads" "$ROOT/titan_workspace" "$ROOT/telemetry"
cd "$ROOT"

if [[ ! -x "$VENV/bin/python" ]]; then
  echo "[SETUP] Creating dashboard virtual environment..."
  python3 -m venv "$VENV"
fi
if [[ ! -f "$VENV/.titan-dependencies-ready" ]]; then
  echo "[SETUP] Installing dashboard and SDK dependencies..."
  "$VENV/bin/python" -m pip install --quiet flask requests pyyaml
  "$VENV/bin/python" -m pip install --quiet -e "$ROOT"
  touch "$VENV/.titan-dependencies-ready"
fi

# Restart only processes that this bundle previously recorded.
stop_pid dashboard
stop_pid master
stop_pid store

echo "[START] TitanStore on 127.0.0.1:6379"
nohup java -jar "$PERM/TitanStore.jar" >"$ROOT/store.log" 2>&1 &
echo $! >"$PID_DIR/store.pid"
sleep 2

echo "[START] Titan Master on 0.0.0.0:9090"
nohup java -cp "$PERM/titan-orchestrator-1.0-SNAPSHOT.jar" titan.TitanMaster >"$ROOT/master.log" 2>&1 &
echo $! >"$PID_DIR/master.pid"
sleep 2

echo "[START] Dashboard on 0.0.0.0:5000"
PYTHONPATH="$ROOT:$PERM" nohup "$VENV/bin/python" "$PERM/server_dashboard.py" >"$ROOT/dashboard.log" 2>&1 &
echo $! >"$PID_DIR/dashboard.pid"
sleep 2

if ! status; then
  echo "[ERROR] One or more services failed. Inspect master.log, store.log, and dashboard.log." >&2
  exit 1
fi

echo "[READY] Master bundle is running."
echo "        Master:    9090"
echo "        Dashboard: 5000"
echo "        Logs:       $ROOT/{master,store,dashboard}.log"
