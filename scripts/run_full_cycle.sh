#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INFRA_DIR="$ROOT_DIR/infra"
BACKEND_DIR="$ROOT_DIR/backend"
API_BASE_URL="${API_BASE_URL:-http://localhost:8080}"
BACKEND_LOG="${BACKEND_LOG:-$ROOT_DIR/run_full_cycle_backend.log}"
RESET_DB="${RESET_DB:-0}"

cleanup() {
  if [[ -n "${BACKEND_PID:-}" ]] && kill -0 "$BACKEND_PID" 2>/dev/null; then
    kill "$BACKEND_PID" || true
    wait "$BACKEND_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

echo "[1/6] Starting Postgres via docker compose..."
(
  cd "$INFRA_DIR"
  if [[ "$RESET_DB" == "1" ]]; then
    docker compose down -v --remove-orphans >/dev/null 2>&1 || true
  fi
  docker compose up -d
)

echo "[2/6] Starting backend (logs: $BACKEND_LOG)..."
(
  cd "$BACKEND_DIR"
  ./gradlew bootRun --no-daemon
) >"$BACKEND_LOG" 2>&1 &
BACKEND_PID=$!

echo "[3/6] Waiting for API to become ready..."
READY=0
for _ in $(seq 1 120); do
  if curl -fsS "$API_BASE_URL/api/status" >/dev/null 2>&1; then
    READY=1
    break
  fi
  sleep 1
done

if [[ "$READY" -ne 1 ]]; then
  echo "Backend did not become ready in time. Tail of log:"
  tail -n 80 "$BACKEND_LOG" || true
  if grep -q "Migration checksum mismatch" "$BACKEND_LOG" 2>/dev/null; then
    echo "Detected Flyway checksum mismatch. Re-run with RESET_DB=1 for a clean local volume."
  fi
  exit 1
fi

echo "[4/6] Running ingest + discovery + crawl endpoints..."
curl -fsS -X POST "$API_BASE_URL/api/ingest"
echo
curl -fsS -X POST "$API_BASE_URL/api/domains/resolve?limit=600"
echo
curl -fsS -X POST "$API_BASE_URL/api/careers/discover?limit=600"
echo
curl -fsS -X POST "$API_BASE_URL/api/crawl/run" \
  -H "Content-Type: application/json" \
  -d '{"companyLimit":50,"resolveLimit":600,"discoverLimit":600,"maxSitemapUrls":200,"maxJobPages":50}'
echo

echo "[5/6] Final status:"
STATUS_JSON="$(curl -fsS "$API_BASE_URL/api/status")"
if command -v jq >/dev/null 2>&1; then
  echo "$STATUS_JSON" | jq .
else
  echo "$STATUS_JSON"
fi

echo "[6/6] Done."
