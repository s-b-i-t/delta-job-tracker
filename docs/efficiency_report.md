# Efficiency report: why this version

## Source and overlap check

Team D implemented an efficiency wrapper on branch `teamd-efficiency-report` (commit `608c999`) that:

- Samples DB truth during a wrapped command.
- Emits a full report directory under `out/<ts>/efficiency_report/` with `metrics.json`, `metrics.csv`, and `README.md`.
- Includes a unit test validating core invariants.

This branch adopts that approach because it is already aligned with our DB snapshot helpers (`scripts/db_snapshot.py`) and produces audit-friendly time-series artifacts.

## What Team B added

To satisfy the E2E proof requirements for a **stable, single-file JSON summary** under `out/<ts>/`, we add:

- `out/<ts>/efficiency_report.json` (`schema_version=efficiency-report-summary-v1`)

This summary is intentionally small and stable (fixed keys), while the full `out/<ts>/efficiency_report/metrics.json` remains the detailed source of truth.

## Known limitations / TODOs

- Rates depend on snapshot cadence and wrapped command duration; very short runs may have sparse samples.
- If the DB snapshot mechanism cannot connect, we still emit `efficiency_report.json` with empty/partial throughput blocks (and `metrics.json` contains the error details).

