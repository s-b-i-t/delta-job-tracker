#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INFRA_DIR="$ROOT_DIR/infra"
BACKEND_DIR="$ROOT_DIR/backend"

OUT_BASE="${OUT_BASE:-$ROOT_DIR/out}"
RESET_DB="${RESET_DB:-0}"
BACKEND_READY_TIMEOUT_SECONDS="${BACKEND_READY_TIMEOUT_SECONDS:-180}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-1}"

STOP_BACKEND=0
API_BASE_URL="${API_BASE_URL:-http://localhost:8080}"
RUNNER_ARGS=()

usage() {
  cat <<'EOF'
Usage:
  ./scripts/run_overnight_stack.sh [options] [-- <run_overnight_crawler args>]

Starts Postgres + backend, waits for readiness, runs overnight crawler, and writes
artifacts + backend log to out/<timestamp>/.

Options:
  --base-url URL        Backend base URL (default: http://localhost:8080)
  --stop-backend        Stop backend process on exit (default: leave running)
  --help                Show this help

Environment:
  RESET_DB=1            Runs docker compose down -v before up
  OUT_BASE=...          Base output directory (default: ./out)
  BACKEND_READY_TIMEOUT_SECONDS=180
  POLL_INTERVAL_SECONDS=1

Examples:
  ./scripts/run_overnight_stack.sh --sec-limit 5000 --resolve-limit 500 --discover-batch-size 50 --num-batches 2 --sleep-between-batches 5
  ./scripts/run_overnight_stack.sh --base-url http://localhost:8082 --sec-limit 5000 --resolve-limit 500
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
ARTIFACT_DIR="$OUT_BASE/$TS"
mkdir -p "$ARTIFACT_DIR"

BACKEND_LOG="$ARTIFACT_DIR/backend.log"
STACK_LOG="$ARTIFACT_DIR/overnight_stack.log"
RUNNER_LOG="$ARTIFACT_DIR/overnight_runner.log"

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

if command -v ss >/dev/null 2>&1; then
  if ss -ltn "( sport = :$SERVER_PORT )" 2>/dev/null | awk 'NR>1 {print $0}' | grep -q .; then
    log "Port $SERVER_PORT is already in use."
    log "Hint: stop the existing service or choose another port with --base-url http://localhost:<port>."
    exit 3
  fi
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
  ./gradlew bootRun --no-daemon --args="--server.port=$SERVER_PORT"
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
