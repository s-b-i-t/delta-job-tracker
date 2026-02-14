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

1. Ingest universe + sample domains:

```bash
curl -X POST http://localhost:8080/api/ingest
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
```

## Endpoints

- `POST /api/ingest`
  - Loads S&P constituents and `data/domains.csv` into `companies` and `company_domains`.
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
  - Returns newest normalized jobs (`limit`, optional `companyId`, optional `ats` filter).

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
