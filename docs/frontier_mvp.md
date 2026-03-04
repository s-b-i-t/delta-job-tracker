# Frontier MVP Runbook

## Prerequisites

1. Postgres available via Docker Compose:

```bash
docker compose -f infra/docker-compose.yml up -d postgres
```

2. Backend started from `backend/`:

```bash
cd backend
./gradlew bootRun
```

3. Optional seed helper script dependencies: Python 3.

## Trigger a Seed Run

### API directly

```bash
curl -sS -X POST "http://localhost:8080/api/frontier/seed?domainLimit=50&maxSitemapFetches=300" | jq .
```

### Helper script

```bash
python3 scripts/run_frontier_seed_mvp.py \
  --base-url http://localhost:8080 \
  --domain-limit 50 \
  --max-sitemap-fetches 300
```

## Metrics to Expect

The seed response includes:
- `seedDomainsRequested`, `seedDomainsUsed`, `hostsSeen`
- `urlsEnqueued`, `sitemapUrlsEnqueued`, `candidateUrlsEnqueued`
- `urlsFetched`, `blockedByBackoff`
- `httpRequestCount`, `http429Count`, `http429Rate`
- `statusBucketCounts`, `queueStatusCounts`

## DB Artifacts

Frontier state is stored in:
- `crawl_hosts`
- `crawl_urls`
- `crawl_url_attempts`

## SQL Sanity Checks

Queue and host health:

```sql
SELECT status, COUNT(*)
FROM crawl_urls
GROUP BY status
ORDER BY status;

SELECT host, inflight_count, backoff_state, next_allowed_at
FROM crawl_hosts
ORDER BY updated_at DESC
LIMIT 20;
```

Most recent attempts and outcomes:

```sql
SELECT a.fetched_at, a.http_status, a.error_bucket, u.url, u.host
FROM crawl_url_attempts a
JOIN crawl_urls u ON u.id = a.url_id
ORDER BY a.fetched_at DESC
LIMIT 50;
```

Sitemap vs candidate queue mix:

```sql
SELECT url_kind, status, COUNT(*)
FROM crawl_urls
GROUP BY url_kind, status
ORDER BY url_kind, status;
```

## MVP Limits to Accept

- Seed selection depends on available company domain quality.
- Host concurrency is intentionally strict (`inflight_count < 1`) for safer rollout.
- Scheduler is optimized for due/lock filtering but can be extended with deeper telemetry later.
