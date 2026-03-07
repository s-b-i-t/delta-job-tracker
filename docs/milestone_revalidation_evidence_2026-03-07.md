# Milestone Re-Validation Evidence 2026-03-07

This document records what is directly verifiable from the preserved local runtime artifact root:

- `/home/bhatt/programming/delta-job-tracker/out/20260307T012003Z_milestone_revalidation`

The code branch for the milestone line is `origin/teama-milestone-revalidation-20260306` at commit `94ac4db`.

## Verified Artifact Presence

The following artifact subtrees are still present locally:

- `prep_canary/20260307T012009Z`
- `lower_profile/20260307T012519Z`
- `recommended_profile/20260307T015804Z`

## Extracted Runtime Evidence

### Lower Profile

Artifact sources:

- `out/20260307T012003Z_milestone_revalidation/lower_profile/20260307T012519Z/ats_scale_metrics.json`
- `out/20260307T012003Z_milestone_revalidation/lower_profile/20260307T012519Z/ats_discovery_final_status.json`
- `out/20260307T012003Z_milestone_revalidation/lower_profile/20260307T012519Z/ats_scale_sql_snapshots.json`

Exact extracted values:

| field | value |
| --- | --- |
| run_status | `ABORTED` |
| stop_reason | `time_budget_exceeded` |
| processedCount | `64` |
| endpointExtractedCount | `35` |
| requestCountTotal | `744` |

Cross-check notes:

- `ats_scale_metrics.json` reports `throughput.requestCount = 744`
- `ats_discovery_final_status.json` reports `status = ABORTED`, `stopReason = time_budget_exceeded`, `processedCount = 64`, `endpointExtractedCount = 35`
- `ats_scale_sql_snapshots.json` reports `run_metrics[0].request_count_total = "744"`

### Recommended Profile

Artifact sources:

- `out/20260307T012003Z_milestone_revalidation/recommended_profile/20260307T015804Z/ats_scale_metrics.json`
- `out/20260307T012003Z_milestone_revalidation/recommended_profile/20260307T015804Z/ats_discovery_final_status.json`
- `out/20260307T012003Z_milestone_revalidation/recommended_profile/20260307T015804Z/ats_scale_sql_snapshots.json`

Exact extracted values:

| field | value |
| --- | --- |
| run_status | `SUCCEEDED` |
| stop_reason | `null` |
| processedCount | `39` |
| endpointExtractedCount | `20` |
| requestCountTotal | `487` |

Cross-check notes:

- `ats_scale_metrics.json` reports `throughput.requestCount = 487`
- `ats_discovery_final_status.json` reports `status = SUCCEEDED`, `stopReason = null`, `processedCount = 39`, `endpointExtractedCount = 20`
- `ats_scale_sql_snapshots.json` reports `run_metrics[0].request_count_total = "487"`

## Methodology Note

This handoff document does not recreate the runtime campaign. It only records what is still provable from the preserved local artifacts.

The preserved runner logs show the two profile commands reaching their own terminal states:

- lower profile log ends with `status=ABORTED`
- recommended profile log ends with `status=SUCCEEDED`

That is consistent with the earlier methodology claim that no external shell timeout shorter than the script wait window undercut those runs. This document preserves the evidence that still exists locally today; it does not claim more than the retained logs and JSON files show.

## Handoff Use

Use this document as the durable pointer for new agents:

- code reality: milestone branch and integrated commit `94ac4db` are real
- runtime reality: the above exact values are still locally recoverable from the preserved artifact root
- limitation: the raw artifact pack is local state, not repo-tracked; this document is the repo-tracked summary of what was verified on 2026-03-07
