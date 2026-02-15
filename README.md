# delta-job-tracker

Backend spike for company job tracking using low-effort crawl signals, automated domain/ATS discovery, and ATS adapters.

## Stack

- Java 21
- Spring Boot + Gradle
- Postgres + Flyway
- JDBC (no ORM)

## Repository layout

- `backend` Spring Boot service
- `infra/docker-compose.yml` Postgres
- `data/sp500_constituents.csv` S&P seed universe
- `data/domains.csv` sample manual domain mapping seed

## Start Postgres

If you run from WSL, enable Docker Desktop WSL integration first:

1. Docker Desktop -> `Settings` -> `Resources` -> `WSL Integration`
2. Enable integration for your distro (for example `Ubuntu`)
3. Confirm `docker info` works inside WSL

```bash
cd infra
docker compose up -d
```

Verify Docker from WSL:

```bash
docker info
```

## Run backend

```bash
cd backend
./gradlew bootRun
```

Default DB connection:

- `jdbc:postgresql://localhost:5432/delta_job_tracker`
- user: `delta`
- password: `delta`

## Recommended V1.1 flow

1. Ingest universe + sample domains (Wikipedia by default):

```bash
curl -X POST http://localhost:8080/api/ingest
```

Fallback to local file source explicitly:

```bash
curl -X POST "http://localhost:8080/api/ingest?source=file"
```

2. Resolve missing domains automatically (Wikidata P856):

```bash
curl -X POST "http://localhost:8080/api/domains/resolve?limit=200"
```

3. Discover careers/ATS endpoints from domains:

```bash
curl -X POST "http://localhost:8080/api/careers/discover?limit=200"
```

4. Run crawl batch:

```bash
curl -X POST http://localhost:8080/api/crawl/run \
  -H 'Content-Type: application/json' \
  -d '{
    "companyLimit": 50,
    "resolveLimit": 200,
    "discoverLimit": 200,
    "maxSitemapUrls": 200,
    "maxJobPages": 50
  }'
```

5. Check status:

```bash
curl http://localhost:8080/api/status
```

6. Inspect newest jobs:

```bash
curl "http://localhost:8080/api/jobs?limit=50"
curl "http://localhost:8080/api/jobs?limit=50&ats=GREENHOUSE"
curl "http://localhost:8080/api/jobs?limit=50&companyId=123"
curl "http://localhost:8080/api/jobs?limit=50&q=software%20engineer"
```

## Endpoints

- `POST /api/ingest`
  - Default source is Wikipedia (`List_of_S%26P_500_companies`), parsed with Jsoup.
  - Supports file fallback via `source=file` using `data/sp500_constituents.csv`.
  - Always ingests `data/domains.csv` after companies as seed/override domains.
  - Returns `companiesUpserted`, `domainsSeeded`, `errorsCount`, and up to 10 `sampleErrors`.
  - `GET /api/ingest` is intentionally not supported and returns `405`.
- `POST /api/domains/resolve?limit=N`
  - Resolves official websites from Wikidata and upserts normalized domains with source metadata.
- `POST /api/careers/discover?limit=N`
  - Probes careers URLs and stores ATS endpoints (`WORKDAY`, `GREENHOUSE`, `LEVER`) when found.
- `POST /api/crawl/run`
  - Orchestrates crawl pipeline:
    1. Optional domain resolution for missing domains
    2. Optional ATS endpoint discovery
    3. Greenhouse/Lever adapter ingestion if ATS endpoint exists
    4. Fallback robots/sitemap/JSON-LD crawl
- `GET /api/status`
  - Returns DB connectivity, key table counts, and latest crawl summary (start/end, jobs extracted, top errors).
- `GET /api/jobs`
  - Returns newest normalized jobs.
  - Filters: `limit`, optional `companyId`, optional `ats`, optional `active`, optional `q` (full-text search).
- `GET /api/jobs/new`
  - Returns jobs with `first_seen_at > since`.
  - Filters: `since` (required), optional `companyId`, optional `limit`, optional `q`.
- `GET /api/jobs/closed`
  - Returns jobs with `is_active=false AND last_seen_at > since`.
  - Filters: `since` (required), optional `companyId`, optional `limit`, optional `q`.

## Search + CS Mode

- Search uses Postgres full-text (`websearch_to_tsquery`) across title, org, location, employment type, and description.
- The static UI includes a **Query** input and a **CS Mode** toggle that fills a preset query for common CS roles while excluding obvious non-CS roles.

## Crawl safeguards

- User agent is always non-empty at runtime (safe fallback default).
- Global concurrency is clamped to `>=1`.
- Per-host delay is clamped to `>=1ms` (default `1000ms`).
- API default company limit is independent from CLI limit (`crawler.api.default-company-limit`).
- Domain resolution and careers discovery limits are independent (`crawler.automation.resolve-limit`, `crawler.automation.discover-limit`).
- Redirect following enabled.
- 403/429 host backoff enabled.
- Robots.txt fetch failure behavior is configurable (`crawler.robots.fail-open`).
- ATS adapter policy when robots is unavailable is configurable (`crawler.robots.allow-ats-adapter-when-unavailable`).
- Sitemap recursion and URL/page fetches are capped.
- WDQS calls are throttled and batched.
- `job_postings.crawl_run_id` uses last-seen attribution: matching postings are updated to the latest crawl run that observed them.

## CLI mode

```bash
cd backend
./gradlew bootRun --args='--spring.main.web-application-type=none --crawler.cli.run=true --crawler.cli.ingest-before-crawl=true --crawler.cli.tickers=WMT,UBER --crawler.cli.limit=2 --crawler.cli.exit-after-run=true'
```

## Test

```bash
cd backend
./gradlew test
```

## Full cycle helper

```bash
./scripts/run_full_cycle.sh
```

The script starts Postgres, starts the backend, runs ingest/resolve/discover/crawl, and prints `/api/status`.
Use `RESET_DB=1 ./scripts/run_full_cycle.sh` if you need a clean local Postgres volume.

## Real Postgres smoke

Terminal 1:

```bash
cd infra
docker compose up -d
cd ../backend
./gradlew clean bootRun
```

Terminal 2:

```bash
curl -X POST http://localhost:8080/api/ingest
curl -X POST "http://localhost:8080/api/domains/resolve?limit=600"
curl -X POST "http://localhost:8080/api/careers/discover?limit=600"
curl -X POST http://localhost:8080/api/crawl/run -H "Content-Type: application/json" -d '{"companyLimit":50,"resolveLimit":600,"discoverLimit":600,"maxSitemapUrls":200,"maxJobPages":50}'
curl http://localhost:8080/api/status
```

Postgres counts:

```bash
docker exec -i delta-job-tracker-postgres psql -U delta -d delta_job_tracker -c "SELECT 'companies' AS table_name, COUNT(*) AS cnt FROM companies UNION ALL SELECT 'company_domains', COUNT(*) FROM company_domains UNION ALL SELECT 'ats_endpoints', COUNT(*) FROM ats_endpoints UNION ALL SELECT 'crawl_runs', COUNT(*) FROM crawl_runs UNION ALL SELECT 'discovered_urls', COUNT(*) FROM discovered_urls UNION ALL SELECT 'job_postings', COUNT(*) FROM job_postings;"
docker exec -i delta-job-tracker-postgres psql -U delta -d delta_job_tracker -c "SELECT COUNT(*) FILTER (WHERE crawl_run_id IS NULL) AS crawl_run_id_null, COUNT(*) FILTER (WHERE crawl_run_id IS NOT NULL) AS crawl_run_id_non_null FROM job_postings;"
```
