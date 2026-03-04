#!/usr/bin/env python3
"""Run Frontier MVP seed flow and print compact metrics.

Example:
  python3 scripts/run_frontier_seed_mvp.py --domain-limit 50 --max-sitemap-fetches 300
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.parse
import urllib.request


def post_json(url: str, timeout: int) -> dict:
    req = urllib.request.Request(url=url, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8")
    return json.loads(body)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Frontier MVP seeding against a running backend")
    parser.add_argument("--base-url", default="http://localhost:8080")
    parser.add_argument("--domain-limit", type=int, default=50)
    parser.add_argument("--max-sitemap-fetches", type=int, default=300)
    parser.add_argument("--timeout-seconds", type=int, default=600)
    args = parser.parse_args()

    base = args.base_url.rstrip("/")
    query = urllib.parse.urlencode(
        {
            "domainLimit": max(1, args.domain_limit),
            "maxSitemapFetches": max(1, args.max_sitemap_fetches),
        }
    )
    url = f"{base}/api/frontier/seed?{query}"

    try:
        payload = post_json(url, timeout=max(1, args.timeout_seconds))
    except Exception as exc:
        print(f"frontier seed failed: {exc}", file=sys.stderr)
        return 1

    hosts_seen = int(payload.get("hostsSeen") or 0)
    urls_enqueued = int(payload.get("urlsEnqueued") or 0)
    urls_fetched = int(payload.get("urlsFetched") or 0)
    blocked_by_backoff = int(payload.get("blockedByBackoff") or 0)
    rate_429 = float(payload.get("http429Rate") or 0.0)

    print(
        "frontier_seed "
        f"hosts_seen={hosts_seen} "
        f"urls_enqueued={urls_enqueued} "
        f"urls_fetched={urls_fetched} "
        f"blocked_by_backoff={blocked_by_backoff} "
        f"rate_429={rate_429:.4f}",
        file=sys.stderr,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
