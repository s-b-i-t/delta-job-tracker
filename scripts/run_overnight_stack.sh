#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INFRA_DIR="$ROOT_DIR/infra"
BACKEND_DIR="$ROOT_DIR/backend"

OUT_BASE="${OUT_BASE:-$ROOT_DIR/out}"
RESET_DB="${RESET_DB:-0}"
BACKEND_READY_TIMEOUT_SECONDS="${BACKEND_READY_TIMEOUT_SECONDS:-180}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-1}"
ISOLATED_STACK_DEFAULT="${DELTA_ISOLATED_STACK:-0}"
DB_HOST_PORT_DEFAULT="${DELTA_DB_PORT:-5432}"
COMPOSE_PROJECT_NAME_DEFAULT="${DELTA_COMPOSE_PROJECT_NAME:-}"
POSTGRES_CONTAINER_NAME_DEFAULT="${DELTA_POSTGRES_CONTAINER_NAME:-}"
POSTGRES_VOLUME_NAME_DEFAULT="${DELTA_POSTGRES_VOLUME_NAME:-}"

STOP_BACKEND=0
API_BASE_URL="${API_BASE_URL:-http://localhost:8080}"
ISOLATED_STACK="$ISOLATED_STACK_DEFAULT"
DB_HOST_PORT="$DB_HOST_PORT_DEFAULT"
COMPOSE_PROJECT_NAME_VALUE="$COMPOSE_PROJECT_NAME_DEFAULT"
POSTGRES_CONTAINER_NAME_VALUE="$POSTGRES_CONTAINER_NAME_DEFAULT"
POSTGRES_VOLUME_NAME_VALUE="$POSTGRES_VOLUME_NAME_DEFAULT"
RUNNER_ARGS=()

usage() {
  cat <<'EOF'
Usage:
  ./scripts/run_overnight_stack.sh [options] [-- <run_overnight_crawler args>]

Starts Postgres + backend, waits for readiness, runs overnight crawler, and writes
artifacts + backend log to out/<timestamp>/.

Options:
  --base-url URL        Backend base URL (default: http://localhost:8080)
  --isolated-stack      Start an isolated compose-backed Postgres stack for this run
  --db-port PORT        Postgres host port to bind (default: 5432, or auto-picked with --isolated-stack)
  --compose-project-name NAME
                        Override compose project name (default: existing compose behavior)
  --stop-backend        Stop backend process on exit (default: leave running)
  --help                Show this help

Environment:
  RESET_DB=1            Runs docker compose down -v before up
  DELTA_ISOLATED_STACK=1
                        Equivalent to --isolated-stack
  DELTA_DB_PORT=...     Override Postgres host port used by docker compose + backend DB_URL
  DELTA_COMPOSE_PROJECT_NAME=...
                        Override compose project name
  DELTA_POSTGRES_CONTAINER_NAME=...
                        Override postgres container name
  DELTA_POSTGRES_VOLUME_NAME=...
                        Override postgres volume name
  OUT_BASE=...          Base output directory (default: ./out)
  BACKEND_READY_TIMEOUT_SECONDS=180
  POLL_INTERVAL_SECONDS=1

Examples:
  ./scripts/run_overnight_stack.sh --sec-limit 5000 --resolve-limit 500 --discover-batch-size 50 --num-batches 2 --sleep-between-batches 5
  ./scripts/run_overnight_stack.sh --base-url http://localhost:8082 --sec-limit 5000 --resolve-limit 500
  ./scripts/run_overnight_stack.sh --isolated-stack --base-url http://localhost:8086 --db-port 55432 --dry-run
  RESET_DB=1 ./scripts/run_overnight_stack.sh --sec-limit 5000 --resolve-limit 500
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --help)
      usage
      exit 0
      ;;
    --stop-backend)
      STOP_BACKEND=1
      shift
      ;;
    --base-url)
      if [[ $# -lt 2 ]]; then
        echo "--base-url requires a value" >&2
        exit 2
      fi
      API_BASE_URL="$2"
      shift 2
      ;;
    --isolated-stack)
      ISOLATED_STACK=1
      shift
      ;;
    --db-port)
      if [[ $# -lt 2 ]]; then
        echo "--db-port requires a value" >&2
        exit 2
      fi
      DB_HOST_PORT="$2"
      shift 2
      ;;
    --compose-project-name)
      if [[ $# -lt 2 ]]; then
        echo "--compose-project-name requires a value" >&2
        exit 2
      fi
      COMPOSE_PROJECT_NAME_VALUE="$2"
      shift 2
      ;;
    --)
      shift
      while [[ $# -gt 0 ]]; do
        RUNNER_ARGS+=("$1")
        shift
      done
      ;;
    *)
      RUNNER_ARGS+=("$1")
      shift
      ;;
  esac
done

if [[ ! "$API_BASE_URL" =~ ^https?://[^/]+$ ]]; then
  echo "Invalid --base-url '$API_BASE_URL'. Expected format: http://host:port" >&2
  exit 2
fi

HOST_PORT="${API_BASE_URL#http://}"
HOST_PORT="${HOST_PORT#https://}"
if [[ "$HOST_PORT" == *:* ]]; then
  SERVER_PORT="${HOST_PORT##*:}"
else
  SERVER_PORT="8080"
fi

if ! [[ "$SERVER_PORT" =~ ^[0-9]+$ ]]; then
  echo "Could not parse server port from --base-url '$API_BASE_URL'" >&2
  exit 2
fi

TS="$(date -u +%Y%m%dT%H%M%SZ)"

pick_free_port() {
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

port_in_use() {
  local port=$1
  if ! command -v ss >/dev/null 2>&1; then
    return 1
  fi
  ss -ltn "( sport = :$port )" 2>/dev/null | awk 'NR>1 {print $0}' | grep -q .
}

if [[ "$ISOLATED_STACK" == "1" && ( -z "$DB_HOST_PORT" || "$DB_HOST_PORT" == "5432" ) ]]; then
  DB_HOST_PORT="$(pick_free_port)"
fi

if ! [[ "$DB_HOST_PORT" =~ ^[0-9]+$ ]]; then
  echo "Invalid --db-port '$DB_HOST_PORT'. Expected an integer port." >&2
  exit 2
fi

if [[ "$ISOLATED_STACK" == "1" ]]; then
  STACK_SUFFIX="${TS,,}"
  if [[ -z "$COMPOSE_PROJECT_NAME_VALUE" ]]; then
    COMPOSE_PROJECT_NAME_VALUE="delta-job-tracker-${STACK_SUFFIX}"
  fi
  if [[ -z "$POSTGRES_CONTAINER_NAME_VALUE" ]]; then
    POSTGRES_CONTAINER_NAME_VALUE="delta-job-tracker-postgres-${STACK_SUFFIX}"
  fi
  if [[ -z "$POSTGRES_VOLUME_NAME_VALUE" ]]; then
    POSTGRES_VOLUME_NAME_VALUE="delta_job_tracker_pg_${STACK_SUFFIX}"
  fi
fi

if [[ -n "$COMPOSE_PROJECT_NAME_VALUE" ]]; then
  export COMPOSE_PROJECT_NAME="$COMPOSE_PROJECT_NAME_VALUE"
fi
export DELTA_POSTGRES_PORT="$DB_HOST_PORT"
if [[ -n "$POSTGRES_CONTAINER_NAME_VALUE" ]]; then
  export DELTA_POSTGRES_CONTAINER_NAME="$POSTGRES_CONTAINER_NAME_VALUE"
fi
if [[ -n "$POSTGRES_VOLUME_NAME_VALUE" ]]; then
  export DELTA_POSTGRES_VOLUME_NAME="$POSTGRES_VOLUME_NAME_VALUE"
fi
export DB_URL="jdbc:postgresql://localhost:${DB_HOST_PORT}/delta_job_tracker"

ARTIFACT_DIR="$OUT_BASE/$TS"
mkdir -p "$ARTIFACT_DIR"

BACKEND_LOG="$ARTIFACT_DIR/backend.log"
STACK_LOG="$ARTIFACT_DIR/overnight_stack.log"
RUNNER_LOG="$ARTIFACT_DIR/overnight_runner.log"
STACK_CONFIG_JSON="$ARTIFACT_DIR/stack_config.json"

log() {
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*" | tee -a "$STACK_LOG" >&2
}

print_backend_failure_hints() {
  log "Backend failed to become ready. Tail of backend log:"
  tail -n 120 "$BACKEND_LOG" >&2 || true

  if grep -Eqi "checksum mismatch|Migration checksum mismatch|failed validation|Validate failed" "$BACKEND_LOG"; then
    log "Hint: Flyway checksum mismatch detected. Re-run with RESET_DB=1 to reset local DB volume."
  fi
  if grep -Eqi "Port [0-9]+ was already in use|Address already in use|BindException" "$BACKEND_LOG"; then
    log "Hint: Port appears in use. Stop the conflicting process or run with --base-url http://localhost:<other-port>."
  fi
  if grep -Eqi "Connection to .* refused|could not connect to server|FATAL:|database .* does not exist" "$BACKEND_LOG"; then
    log "Hint: Database connectivity failure detected. Ensure docker is running and Postgres is healthy."
  fi
}

BACKEND_PID=""
cleanup() {
  if [[ "$STOP_BACKEND" == "1" ]] && [[ -n "$BACKEND_PID" ]] && kill -0 "$BACKEND_PID" 2>/dev/null; then
    log "Stopping backend (pid=$BACKEND_PID)"
    kill "$BACKEND_PID" || true
    wait "$BACKEND_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

log "Artifacts directory: $ARTIFACT_DIR"
log "Effective readiness settings: BACKEND_READY_TIMEOUT_SECONDS=${BACKEND_READY_TIMEOUT_SECONDS}s POLL_INTERVAL_SECONDS=${POLL_INTERVAL_SECONDS}s"
python3 - <<PY >"$STACK_CONFIG_JSON"
import json
print(json.dumps({
    "isolated_stack": ${ISOLATED_STACK},
    "api_base_url": ${API_BASE_URL@Q},
    "server_port": int(${SERVER_PORT@Q}),
    "db_url": ${DB_URL@Q},
    "db_host_port": int(${DB_HOST_PORT@Q}),
    "compose_project_name": ${COMPOSE_PROJECT_NAME_VALUE@Q},
    "postgres_container_name": ${POSTGRES_CONTAINER_NAME_VALUE@Q},
    "postgres_volume_name": ${POSTGRES_VOLUME_NAME_VALUE@Q},
}, indent=2))
PY
log "Stack config: isolated=${ISOLATED_STACK} compose_project=${COMPOSE_PROJECT_NAME_VALUE:-<default>} db_port=${DB_HOST_PORT} container=${POSTGRES_CONTAINER_NAME_VALUE:-delta-job-tracker-postgres} volume=${POSTGRES_VOLUME_NAME_VALUE:-delta_job_tracker_pg}"
if [[ "${#RUNNER_ARGS[@]}" -gt 0 ]]; then
  log "Runner passthrough args: ${RUNNER_ARGS[*]}"
else
  log "Runner passthrough args: <none>"
fi

if port_in_use "$SERVER_PORT"; then
  log "Port $SERVER_PORT is already in use."
  log "Hint: stop the existing service or choose another port with --base-url http://localhost:<port>."
  exit 3
fi

if [[ "$ISOLATED_STACK" == "1" ]] && port_in_use "$DB_HOST_PORT"; then
  log "DB host port $DB_HOST_PORT is already in use."
  log "Hint: choose another --db-port or let --isolated-stack auto-pick one."
  exit 3
fi

log "Starting Postgres via docker compose..."
(
  cd "$INFRA_DIR"
  if [[ "$RESET_DB" == "1" ]]; then
    docker compose down -v --remove-orphans >/dev/null 2>&1 || true
  fi
  docker compose up -d
) || {
  log "Failed to start Postgres via docker compose."
  log "Hint: ensure Docker Desktop/daemon is running and WSL integration is enabled."
  exit 3
}

log "Starting backend on port $SERVER_PORT (log: $BACKEND_LOG)..."
(
  cd "$BACKEND_DIR"
  DB_URL="$DB_URL" ./gradlew bootRun --no-daemon --args="--server.port=$SERVER_PORT"
) >"$BACKEND_LOG" 2>&1 &
BACKEND_PID=$!

log "Waiting for backend readiness at $API_BASE_URL/api/status (timeout=${BACKEND_READY_TIMEOUT_SECONDS}s)..."
READY=0
for ((i=1; i<=BACKEND_READY_TIMEOUT_SECONDS; i++)); do
  if curl -fsS "$API_BASE_URL/api/status" >"$ARTIFACT_DIR/api_status_ready.json" 2>/dev/null; then
    READY=1
    break
  fi
  if ! kill -0 "$BACKEND_PID" 2>/dev/null; then
    break
  fi
  sleep "$POLL_INTERVAL_SECONDS"
done

if [[ "$READY" -ne 1 ]]; then
  print_backend_failure_hints
  exit 4
fi

log "Backend is ready. Running overnight crawler..."
log "Command: python3 scripts/run_overnight_crawler.py --base-url $API_BASE_URL --out-dir $ARTIFACT_DIR ${RUNNER_ARGS[*]:-}"
set +e
python3 "$ROOT_DIR/scripts/run_overnight_crawler.py" --base-url "$API_BASE_URL" --out-dir "$ARTIFACT_DIR" "${RUNNER_ARGS[@]}" 2>&1 | tee "$RUNNER_LOG"
RUNNER_RC=${PIPESTATUS[0]}
set -e

# Overnight runner writes into a timestamped subdir under --out-dir. Flatten to ARTIFACT_DIR.
INNER_DIR="$(find "$ARTIFACT_DIR" -mindepth 1 -maxdepth 1 -type d | head -n 1 || true)"
if [[ -n "$INNER_DIR" && -d "$INNER_DIR" ]]; then
  shopt -s nullglob dotglob
  for f in "$INNER_DIR"/*; do
    mv "$f" "$ARTIFACT_DIR"/
  done
  shopt -u nullglob dotglob
  rmdir "$INNER_DIR" || true
fi

if [[ ! -f "$ARTIFACT_DIR/overnight_summary.json" || ! -f "$ARTIFACT_DIR/canary_summary.json" ]]; then
  log "Runner finished (rc=$RUNNER_RC) but expected artifacts were not found in $ARTIFACT_DIR"
  ls -la "$ARTIFACT_DIR" >&2 || true
  exit 5
fi

if [[ "$RUNNER_RC" -ne 0 ]]; then
  log "Overnight runner exited with code $RUNNER_RC"
  log "Artifacts available at: $ARTIFACT_DIR"
  exit "$RUNNER_RC"
fi

log "Overnight stack run complete."
log "Artifacts: $ARTIFACT_DIR"
if [[ "$STOP_BACKEND" == "0" ]]; then
  log "Backend left running (pid=$BACKEND_PID). Use --stop-backend to stop automatically."
fi
