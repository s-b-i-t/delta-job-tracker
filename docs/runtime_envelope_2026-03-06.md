# Runtime Envelope Sweep (2026-03-06)

## Branch
- Branch: `teama-runtime-envelope-20260306`
- Base: `origin/teama-deploy-candidate-20260306` (`cbf344d`)
- Scope: runtime parameter envelope only (no product logic edits)

## Runtime setup (fresh live)
```bash
docker compose -f infra/docker-compose.yml down
docker compose -f infra/docker-compose.yml up -d
docker compose -f infra/docker-compose.yml ps
docker exec delta-job-tracker-postgres pg_isready -U app -d delta_job_tracker

cd backend && ./gradlew bootRun
```

## Sweep approach (high-signal, bounded)
Primary structured grid (fixed max-wait stress point):
- `domain-limit`: 50, 120, 200
- `discover-limit`: 100, 200, 300
- `discover-batch-size`: 25, 25, 50
- `max-sitemap-fetches`: 50, 80, 120
- `max-wait-seconds`: 240 (intentionally tight to detect instability threshold)

Control point:
- same low profile as `safe` but `max-wait-seconds=900` to test whether longer wait restores completion.

## Exact commands run
```bash
# Structured sweep (fixed window)
python3 scripts/run_ats_scale_canary.py --out-dir out/20260306T223256Z_runtime_envelope_sweep_fixed/safe_out --domain-limit 50 --max-sitemap-fetches 50 --discover-limit 100 --discover-batch-size 25 --max-wait-seconds 240 --http-timeout-seconds 300 --postgres-user app
python3 scripts/run_ats_scale_canary.py --out-dir out/20260306T223256Z_runtime_envelope_sweep_fixed/mid_out --domain-limit 120 --max-sitemap-fetches 80 --discover-limit 200 --discover-batch-size 25 --max-wait-seconds 240 --http-timeout-seconds 300 --postgres-user app
python3 scripts/run_ats_scale_canary.py --out-dir out/20260306T223256Z_runtime_envelope_sweep_fixed/high_out --domain-limit 200 --max-sitemap-fetches 120 --discover-limit 300 --discover-batch-size 50 --max-wait-seconds 240 --http-timeout-seconds 300 --postgres-user app

# Control run (extended wait)
python3 scripts/run_ats_scale_canary.py --out-dir out/20260306T224836Z_runtime_envelope_control --domain-limit 50 --max-sitemap-fetches 50 --discover-limit 100 --discover-batch-size 25 --max-wait-seconds 900 --http-timeout-seconds 300 --postgres-user app
```

## Artifact paths
- Sweep root: `out/20260306T223256Z_runtime_envelope_sweep_fixed`
  - `configs.csv`
  - `safe.stderr.log`, `safe.result.json`, `safe_out/20260306T223256Z/*`
  - `mid.stderr.log`, `mid.result.json`, `mid_out/20260306T223717Z/*`
  - `high.stderr.log`, `high.result.json`, `high_out/20260306T224145Z/*`
- Control root: `out/20260306T224836Z_runtime_envelope_control`
  - `control.stderr.log`, `control.stdout.json`, `20260306T224836Z/*`

## Envelope table (actionable)
| Config | Completed? | Key outputs observed | Blocker / failure mode |
|---|---|---|---|
| `safe`: d=50, s=50, dl=100, bs=25, wait=240 | No | Frontier completed (`hostsSeen=50`, `urlsEnqueued=0`, `urlsFetched=50`); discovery progressed to `processed=10`, `endpoint_extracted=3` by 240s | `RuntimeError: Timed out waiting for discovery run` |
| `mid`: d=120, s=80, dl=200, bs=25, wait=240 | No | Frontier completed (`hostsSeen=120`, `urlsEnqueued=193`, `urlsFetched=80`); discovery reached `processed=22`, `endpoint_extracted=6` | `RuntimeError: Timed out waiting for discovery run` |
| `high`: d=200, s=120, dl=300, bs=50, wait=240 | No | Frontier completed (`hostsSeen=200`, `urlsEnqueued=308`, `urlsFetched=120`); discovery reached `processed=22`, `endpoint_extracted=13` | `RuntimeError: Timed out waiting for discovery run` |
| `control_900`: d=50, s=50, dl=100, bs=25, wait=900 | No | Frontier completed (`hostsSeen=50`, `urlsEnqueued=783`, `http429Rate=0.024`); discovery reached `processed=43`, `endpoint_extracted=26` by ~901s | `RuntimeError: Timed out waiting for discovery run` at wait limit |

## Earliest practical stability threshold (from fresh evidence)
- At `max-wait=240`, all tested profiles time out before discovery run completion.
- Extending to `max-wait=900` improves progress materially but still does **not** complete for the tested low profile.
- Practical threshold where degradation is first clearly visible: **`max-wait <= 240` is too low** for this integrated candidate; even 900 may be insufficient for guaranteed completion under these limits.

## Final runtime-envelope verdict
### Currently safe bounded settings (fresh evidence)
- **Frontier seed stage** is stable through at least:
  - `domain-limit=200`
  - `max-sitemap-fetches=120`
- **Discovery completion safety** not yet achieved in tested sweep (no fully completed scale-canary run).

### Unsafe / not-yet-viable settings
- Any tested scale-canary profile with `max-wait=240` (all timed out).
- Low profile with `max-wait=900` is also not yet completion-safe.

### Recommended next larger bounded config for final validation
Use this next step once PM approves one more bounded run:
- `domain-limit=120`
- `max-sitemap-fetches=80`
- `discover-limit=200`
- `discover-batch-size=25`
- `max-wait-seconds=1800`
- `http-timeout-seconds=300`
- `postgres-user=app`

Rationale:
- `mid` frontier behavior was stable and representative.
- `mid` discovery throughput was non-trivial even under the constrained 240s window.
- Increasing wait window is the highest-leverage parameter based on this envelope data.
