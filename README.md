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

3. Discover careers/ATS endpoints from domains (async):

```bash
curl -X POST "http://localhost:8080/api/careers/discover?limit=200"
curl http://localhost:8080/api/careers/discover/runs/latest
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

Optional one-button full cycle (resolve -> discover -> crawl):

```bash
curl -X POST "http://localhost:8080/api/automation/full-cycle?companies=200&discoverLimit=200&crawlLimit=200"
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
  - SEC ingest stores CIK as a zero-padded 10-digit string when available.
  - Always ingests `data/domains.csv` after companies as seed/override domains.
  - Returns `companiesUpserted`, `domainsSeeded`, `errorsCount`, and up to 10 `sampleErrors`.
  - `GET /api/ingest` is intentionally not supported and returns `405`.
- `POST /api/universe/ingest/sec?limit=N`
  - Ingests SEC company tickers into `companies` with `CIK`, `ticker`, and `name`.
  - Returns `inserted`, `updated`, `total`, and `errorCount` with sample errors.
  - Uses `crawler.data.sec-user-agent` (env `CRAWLER_SEC_USER_AGENT`) when set; falls back to `crawler.user-agent`.
- `POST /api/canary/sec?limit=N`
  - Starts an async SEC canary run and returns `{runId, status}` quickly.
- `POST /api/canary/sec-full-cycle?companyLimit=N&discoverVendorProbeOnly=true&crawl=true`
  - Runs SEC ingest → domain resolution → ATS discovery → ATS crawl (atsOnly=true) with canary budgets.
- `GET /api/canary/{runId}`
  - Returns run status and the latest SEC canary summary when available.
- `GET /api/canary/latest?type=SEC`
  - Returns the most recent canary run and its latest summary. `type` is optional; when omitted the latest run of any type is returned.
- `GET /api/hosts/cooldown`
  - Lists hosts currently in cooldown with `next_allowed_at` timestamps.
- `POST /api/domains/resolve?limit=N`
  - Resolves official websites from Wikidata (Wikipedia title first, then CIK) and upserts normalized domains with source metadata.
- `GET /api/diagnostics/missing-domains?limit=N`
  - Lists companies still missing domains with cached resolution reasons and a reason breakdown.
- `POST /api/careers/discover?limit=N&vendorProbeOnly=true`
  - Starts an async discovery run and returns `{runId, status, statusUrl}`.
  - `vendorProbeOnly=true` skips homepage/careers path fetches and only probes ATS vendor hosts.
- `GET /api/careers/discover/run/{id}`
  - Returns discovery run status + metrics.
- `GET /api/careers/discover/runs/latest`
  - Returns the most recent discovery run status.
- `POST /api/crawl/run`
  - Orchestrates crawl pipeline:
    1. Optional domain resolution for missing domains
    2. Optional ATS endpoint discovery
    3. Greenhouse/Lever adapter ingestion if ATS endpoint exists
    4. Fallback robots/sitemap/JSON-LD crawl
- `POST /api/automation/full-cycle`
  - Runs resolve -> discover -> crawl in one call.
  - Query params: `companies`, optional `resolveLimit`, `discoverLimit`, `crawlLimit`, `maxJobPages`, `maxSitemapUrls`.
- `GET /api/status`
  - Returns DB connectivity, key table counts, and latest crawl summary (start/end, jobs extracted, top errors).
- `GET /api/diagnostics/coverage`
  - Returns counts for `company_domains`, `discovered_urls`, `ats_endpoints`, `job_postings`, plus `atsEndpointsByType`.
- `GET /api/diagnostics/discovery-failures`
  - Returns counts by `reason_code` and the 20 most recent discovery failures with ticker/name and URL detail.
- `GET /api/jobs`
  - Returns newest normalized jobs.
  - Filters: `limit`, optional `companyId`, optional `ats`, optional `active`, optional `q` (full-text search).
- `GET /api/jobs/new`
  - Returns jobs with `first_seen_at > since`.
  - Filters: `since` (required), optional `companyId`, optional `limit`, optional `q`.
- `GET /api/jobs/closed`
  - Returns jobs with `is_active=false AND last_seen_at > since`.
  - Filters: `since` (required), optional `companyId`, optional `limit`, optional `q`.
- `GET /api/jobs/{id}`
  - Returns full job detail including `descriptionText`.
- `GET /api/companies`
  - Search companies by ticker/name/domain: `?search=air&limit=20`.

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
- Host cooldown persists across runs for repeated timeouts/robots/429s.
- Robots.txt fetch failure behavior is configurable (`crawler.robots.fail-open`).
- ATS adapter policy when robots is unavailable is configurable (`crawler.robots.allow-ats-adapter-when-unavailable`).
- Sitemap recursion and URL/page fetches are capped.
- WDQS calls are throttled and batched.
- `job_postings.crawl_run_id` uses last-seen attribution: matching postings are updated to the latest crawl run that observed them.

## Canonical URL refresh

- New crawls update `canonical_url` (and `source_url`) when a posting with the same external identifier is observed.
- If you have older rows pointing at ATS JSON endpoints, re-run the crawl for those companies to refresh URLs.

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

## Flyway validation

Do not disable Flyway validation (`SPRING_FLYWAY_VALIDATE_ON_MIGRATE=false` is not supported).
If you previously edited an applied migration (for example `V16__crawl_run_company_results_observability.sql`) and see a checksum mismatch:

- Recommended (dev only): reset the local database volume (for example `RESET_DB=1 ./scripts/run_full_cycle.sh`) and rerun.
- Alternative (dev only): run `./gradlew flywayRepair` to align checksums **only** if you are sure the schema matches the migration history.

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

## How to verify locally

- Missing domains diagnostics: `curl "http://localhost:8080/api/diagnostics/missing-domains?limit=50"`
- Resolve remaining domains only: `curl -X POST "http://localhost:8080/api/domains/resolve?limit=600"` (rerun until coverage > 98%).
- Canary latest (type optional): `curl "http://localhost:8080/api/canary/latest"` or `curl "http://localhost:8080/api/canary/latest?type=SEC"`

Postgres counts:

```bash
docker exec -i delta-job-tracker-postgres psql -U delta -d delta_job_tracker -c "SELECT 'companies' AS table_name, COUNT(*) AS cnt FROM companies UNION ALL SELECT 'company_domains', COUNT(*) FROM company_domains UNION ALL SELECT 'ats_endpoints', COUNT(*) FROM ats_endpoints UNION ALL SELECT 'crawl_runs', COUNT(*) FROM crawl_runs UNION ALL SELECT 'discovered_urls', COUNT(*) FROM discovered_urls UNION ALL SELECT 'job_postings', COUNT(*) FROM job_postings;"
docker exec -i delta-job-tracker-postgres psql -U delta -d delta_job_tracker -c "SELECT COUNT(*) FILTER (WHERE crawl_run_id IS NULL) AS crawl_run_id_null, COUNT(*) FILTER (WHERE crawl_run_id IS NOT NULL) AS crawl_run_id_non_null FROM job_postings;"
```
