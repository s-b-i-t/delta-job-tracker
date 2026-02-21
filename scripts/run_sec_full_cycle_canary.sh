#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/run_sec_full_cycle_canary.sh [options]

Options:
  --base-url URL                 Base URL for backend (default: http://localhost:8080)
  --company-limit N              Company limit (default: 50)
  --crawl true|false             Run ATS crawl step (default: true)
  --vendor-probe-only true|false Discovery vendor-probe only (default: false)
  --max-wait-seconds N           Max time to wait for completion (default: 900)
  --poll-interval-seconds N      Poll interval (default: 5)
  --compose-up                   Run docker compose up -d before canary (default: false)
  --compose-file PATH            Compose file path (default: infra/docker-compose.yml)
  -h, --help                     Show help

Environment variables (override defaults):
  DELTA_BASE_URL
  DELTA_COMPANY_LIMIT
  DELTA_CRAWL
  DELTA_VENDOR_PROBE_ONLY
  DELTA_MAX_WAIT_SECONDS
  DELTA_POLL_INTERVAL_SECONDS
  DELTA_COMPOSE_UP
  DELTA_COMPOSE_FILE
USAGE
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Missing required command: $cmd" >&2
    exit 1
  fi
}

BASE_URL="${DELTA_BASE_URL:-http://localhost:8080}"
COMPANY_LIMIT="${DELTA_COMPANY_LIMIT:-50}"
CRAWL="${DELTA_CRAWL:-true}"
VENDOR_PROBE_ONLY="${DELTA_VENDOR_PROBE_ONLY:-false}"
MAX_WAIT_SECONDS="${DELTA_MAX_WAIT_SECONDS:-900}"
POLL_INTERVAL_SECONDS="${DELTA_POLL_INTERVAL_SECONDS:-5}"
COMPOSE_UP="${DELTA_COMPOSE_UP:-false}"
COMPOSE_FILE="${DELTA_COMPOSE_FILE:-infra/docker-compose.yml}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base-url) BASE_URL="${2:-}"; shift 2 ;;
    --company-limit) COMPANY_LIMIT="${2:-}"; shift 2 ;;
    --crawl) CRAWL="${2:-}"; shift 2 ;;
    --vendor-probe-only) VENDOR_PROBE_ONLY="${2:-}"; shift 2 ;;
    --max-wait-seconds) MAX_WAIT_SECONDS="${2:-}"; shift 2 ;;
    --poll-interval-seconds) POLL_INTERVAL_SECONDS="${2:-}"; shift 2 ;;
    --compose-up) COMPOSE_UP=true; shift 1 ;;
    --compose-file) COMPOSE_FILE="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

require_cmd curl
require_cmd jq

if [[ "$COMPOSE_UP" == "true" ]]; then
  if command -v docker >/dev/null 2>&1; then
    if docker compose version >/dev/null 2>&1; then
      docker compose -f "$COMPOSE_FILE" up -d
    elif command -v docker-compose >/dev/null 2>&1; then
      docker-compose -f "$COMPOSE_FILE" up -d
    else
      echo "docker is present but neither 'docker compose' nor 'docker-compose' is available" >&2
      exit 1
    fi
  else
    echo "Missing required command for --compose-up: docker" >&2
    exit 1
  fi
fi

start_url="$BASE_URL/api/canary/sec-full-cycle?companyLimit=$COMPANY_LIMIT&discoverVendorProbeOnly=$VENDOR_PROBE_ONLY&crawl=$CRAWL"
echo "Starting SEC full-cycle canary: $start_url" >&2
start_resp="$(curl -sS -X POST "$start_url")"
run_id="$(jq -r '.runId // empty' <<<"$start_resp")"
if [[ -z "$run_id" || "$run_id" == "null" ]]; then
  echo "Failed to parse runId from response:" >&2
  echo "$start_resp" | jq . >&2 || true
  exit 1
fi
echo "runId=$run_id" >&2

deadline="$(( $(date +%s) + MAX_WAIT_SECONDS ))"
last_status=""
status_resp=""
while true; do
  now="$(date +%s)"
  if (( now > deadline )); then
    echo "Timed out waiting for canary completion (runId=$run_id)" >&2
    break
  fi

  status_resp="$(curl -sS "$BASE_URL/api/canary/$run_id")"
  status="$(jq -r '.status // empty' <<<"$status_resp")"
  if [[ -n "$status" && "$status" != "$last_status" ]]; then
    echo "status=$status" >&2
    last_status="$status"
  fi

  if [[ -n "$status" && "$status" != "RUNNING" ]]; then
    break
  fi
  sleep "$POLL_INTERVAL_SECONDS"
done

echo "" >&2
echo "Final canary response:" >&2
echo "$status_resp" | jq .

echo "" >&2
echo "Final canary summary:" >&2
echo "$status_resp" | jq '.summary'

echo "" >&2
echo "Coverage diagnostics:" >&2
curl -sS "$BASE_URL/api/diagnostics/coverage" | jq .

