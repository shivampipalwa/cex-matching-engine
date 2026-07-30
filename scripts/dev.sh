#!/usr/bin/env bash
# Local dev environment: bring up redis + postgres, optionally the 3 bins.
#
# Usage:
#   ./scripts/dev.sh up          # start redis + postgres only
#   ./scripts/dev.sh up --bins   # also build + start engine, db_writer, api
#   ./scripts/dev.sh down        # stop bins started by this script (if any)
#   ./scripts/dev.sh status      # show what's currently running
set -euo pipefail

REDIS=trusting_lamport   # redis container name
PG=pg                    # postgres container name
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PIDFILE="$ROOT/.dev-pids"
cd "$ROOT"

wait_for() { # poll a command until it succeeds, up to ~15s
  for _ in $(seq 1 50); do "$@" >/dev/null 2>&1 && return 0; sleep 0.3; done
  echo "timed out waiting for: $*" >&2; exit 1
}

infra_up() {
  echo "== redis + postgres =="
  docker start "$PG" "$REDIS" >/dev/null
  wait_for docker exec "$PG" pg_isready -U postgres
  wait_for docker exec "$REDIS" redis-cli ping
  echo "  ✓ postgres + redis up"
}

bins_up() {
  set -a; source .env; set +a
  echo "== build + start bins =="
  cargo build --bins -q
  : > "$PIDFILE"
  ./target/debug/engine    >/tmp/dev_engine.log    2>&1 & echo "engine $!"    >> "$PIDFILE"
  ./target/debug/db_writer >/tmp/dev_dbwriter.log  2>&1 & echo "db_writer $!" >> "$PIDFILE"
  wait_for bash -c "docker exec $REDIS redis-cli XINFO GROUPS commands | grep -q engine-group"
  ./target/debug/api       >/tmp/dev_api.log       2>&1 & echo "api $!"       >> "$PIDFILE"
  wait_for curl -s -o /dev/null http://127.0.0.1:3000/
  echo "  ✓ engine, db_writer, api up (logs in /tmp/dev_*.log)"
}

down() {
  [[ -f "$PIDFILE" ]] || { echo "no bins tracked (no $PIDFILE)"; return 0; }
  echo "== stopping bins =="
  while read -r name pid; do
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" && echo "  ✓ stopped $name (pid $pid)"
    else
      echo "  - $name (pid $pid) already gone"
    fi
  done < "$PIDFILE"
  rm -f "$PIDFILE"
}

status() {
  echo "== containers =="
  docker ps --filter "name=$PG" --filter "name=$REDIS" --format '  {{.Names}}: {{.Status}}'
  echo "== bins =="
  if [[ -f "$PIDFILE" ]]; then
    while read -r name pid; do
      if kill -0 "$pid" 2>/dev/null; then echo "  $name: running (pid $pid)"
      else echo "  $name: not running"
      fi
    done < "$PIDFILE"
  else
    echo "  none tracked (no $PIDFILE)"
  fi
}

case "${1:-}" in
  up)     infra_up; if [[ "${2:-}" == "--bins" ]]; then bins_up; fi ;;
  down)   down ;;
  status) status ;;
  *) echo "usage: $0 {up [--bins]|down|status}" >&2; exit 1 ;;
esac
