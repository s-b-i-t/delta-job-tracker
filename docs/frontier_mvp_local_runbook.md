# Frontier MVP Local Runbook (Postgres)

This runbook validates the Team 2 Frontier MVP against the real Postgres stack.

## Prerequisites

- Docker with `docker compose`
- Python 3
- Java 21

## 1) Start Postgres

```bash
cd infra
docker compose up -d
cd ..
```

## 2) Start backend (master profile / Postgres datasource)

```bash
cd backend
./gradlew bootRun --no-daemon --args="--server.port=8080"
```

Keep this process running.

## 3) Run frontier seeding

```bash
python3 scripts/run_frontier_seed_mvp.py --domain-limit 50 --max-sitemap-fetches 50
```

The script writes:
- one-line stderr summary: `frontier_seed hosts_seen=... urls_enqueued=... urls_fetched=... blocked_by_backoff=... rate_429=...`
- full JSON payload to stdout

## Frontier robots policy (explicit)

Config key: `crawler.frontier.respect-robots-for-sitemaps`  
Default: `true`

What it changes:
- `true`: frontier sitemap fetches are blocked when `robots.txt` disallows the sitemap URL; those are recorded as `blocked_by_robots`/`ROBOTS_BLOCKED`.
- `false`: frontier does not apply the robots allow check before sitemap fetch in scheduler; requests still go through `PoliteHttpClient` rate limiting and host backoff.

You can override for local runs via env:

```bash
CRAWLER_FRONTIER_RESPECT_ROBOTS_FOR_SITEMAPS=false
```

## 4) SQL verification after run

```bash
cd infra
docker compose exec -T postgres psql -U delta -d delta_job_tracker -c "\
SELECT\
  (SELECT COUNT(*) FROM crawl_hosts) AS crawl_hosts_count,\
  (SELECT COUNT(*) FROM crawl_urls) AS crawl_urls_count,\
  (SELECT COUNT(*) FROM crawl_url_attempts) AS crawl_url_attempts_count;\
"
```

Optional deeper verification:

```bash
cd infra
docker compose exec -T postgres psql -U delta -d delta_job_tracker -c "\
SELECT url_kind, status, COUNT(*)\
FROM crawl_urls\
GROUP BY url_kind, status\
ORDER BY url_kind, status;\
"
```

## 5) Optional JSON field extraction

If you saved script stdout to `frontier_seed_output.json`:

```bash
jq '{hostsSeen, urlsEnqueued, urlsFetched, blockedByBackoff, http429Rate}' frontier_seed_output.json
```
