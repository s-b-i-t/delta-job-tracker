# Discovery Completion Envelope (2026-03-06)

## Branch
- Branch: `teama-discovery-envelope-20260306`
- Base: `origin/teama-deploy-candidate-20260306` @ `cbf344d`
- Scope: runtime-only discovery-completion envelope mapping

## Runtime setup steps
```bash
docker compose -f infra/docker-compose.yml down
docker compose -f infra/docker-compose.yml up -d
docker compose -f infra/docker-compose.yml ps
docker exec delta-job-tracker-postgres pg_isready -U app -d delta_job_tracker
cd backend && ./gradlew bootRun
```

## Structured focused sweep
Profiles executed:
1. lower (safer): d=80, s=60, dl=150, bs=20, wait=900
2. recommended (PM-required): d=120, s=80, dl=200, bs=25, wait=1800
3. aggressive (slightly higher): d=160, s=100, dl=260, bs=30, wait=900

## Exact commands run
```bash
# lower
timeout 1200 python3 scripts/run_ats_scale_canary.py \
  --out-dir out/20260306T233744Z_discovery_completion_envelope/lower_out \
  --domain-limit 80 --max-sitemap-fetches 60 \
  --discover-limit 150 --discover-batch-size 20 \
  --max-wait-seconds 900 --http-timeout-seconds 300 --postgres-user app

# recommended
timeout 1200 python3 scripts/run_ats_scale_canary.py \
  --out-dir out/20260306T233744Z_discovery_completion_envelope/recommended_out \
  --domain-limit 120 --max-sitemap-fetches 80 \
  --discover-limit 200 --discover-batch-size 25 \
  --max-wait-seconds 1800 --http-timeout-seconds 300 --postgres-user app

# aggressive
timeout 1200 python3 scripts/run_ats_scale_canary.py \
  --out-dir out/20260306T233744Z_discovery_completion_envelope/aggressive_out \
  --domain-limit 160 --max-sitemap-fetches 100 \
  --discover-limit 260 --discover-batch-size 30 \
  --max-wait-seconds 900 --http-timeout-seconds 300 --postgres-user app
```

For each run, after bounded observation ended, status snapshot was captured from:
- `GET /api/careers/discover/run/<run_id>`

## Artifact paths (fresh only)
- Sweep root: `out/20260306T233744Z_discovery_completion_envelope`
- lower:
  - `out/20260306T233744Z_discovery_completion_envelope/lower.stderr.log`
  - `out/20260306T233744Z_discovery_completion_envelope/lower_out/20260306T233751Z/frontier_seed_response.json`
  - `out/20260306T233744Z_discovery_completion_envelope/lower_out/20260306T233751Z/status_at_exit.json`
- recommended:
  - `out/20260306T233744Z_discovery_completion_envelope/recommended.stderr.log`
  - `out/20260306T233744Z_discovery_completion_envelope/recommended_out/20260306T234935Z/frontier_seed_response.json`
  - `out/20260306T233744Z_discovery_completion_envelope/recommended_out/20260306T234935Z/status_at_exit.json`
- aggressive:
  - `out/20260306T233744Z_discovery_completion_envelope/aggressive.stderr.log`
  - `out/20260306T233744Z_discovery_completion_envelope/aggressive_out/20260306T235607Z/frontier_seed_response.json`
  - `out/20260306T233744Z_discovery_completion_envelope/aggressive_out/20260306T235607Z/status_at_exit.json`

## Refined discovery-completion envelope table
| Config | Frontier complete? | Discovery complete? | Key outputs at exit | Failure / stall mode |
|---|---|---|---|---|
| lower: d80/s60/dl150/bs20/w900 | Yes | No | processed=37/150, endpointExtracted=14, requestCountTotal=432 | Stayed `RUNNING`; progress slowed heavily (last poll line: processed 34 at 541s) |
| recommended: d120/s80/dl200/bs25/w1800 | Yes | No | processed=22/200, endpointExtracted=4, requestCountTotal=164 | Stayed `RUNNING`; early stall window by ~300s |
| aggressive: d160/s100/dl260/bs30/w900 | Yes | No | processed=19/260, endpointExtracted=3, requestCountTotal=125 | Stayed `RUNNING`; low throughput by ~330s |

## Final verdict
### Currently safe bounded discovery-completion settings
- No fully safe discovery-completion setting proven yet in this focused scale-canary sweep.
- Frontier seed remains successful across all tested profiles.

### Still unsafe settings
- All tested discovery profiles above remained `RUNNING` (incomplete) within bounded observation windows.
- Discovery completion is still the limiting factor.

### Best next larger bounded config after this cycle
Run this next with a full uninterrupted window:
- `domain-limit=120`
- `max-sitemap-fetches=80`
- `discover-limit=200`
- `discover-batch-size=25`
- `max-wait-seconds=1800`
- `http-timeout-seconds=300`
- `postgres-user=app`

Rationale: it is the PM-designated target and currently shows the key completion bottleneck behavior to close before larger bounded ingestion.
