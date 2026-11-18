"""AC-W-09: monitoring is live. Run with the stack up: ``make check-monitoring``.

- Prometheus: every target of the ``tickdb-ingest`` job is up; the alert rules loaded with
  health ok.
- Grafana: both dashboards are provisioned, and every panel query runs without an error
  through /api/ds/query (template variables expanded to all venues, the dashboard's time range).
- ClickHouse (through Grafana's read-only datasource): ``dq_daily`` has a row for yesterday.

Reads GRAFANA_ADMIN_PASSWORD from the environment (``make`` loads .env) and never prints it.
Prints one JSON summary; exit 1 on any failure.
"""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

VENUES = ("binance-usdm", "coinbase", "kraken")


def expand(text: str) -> str:
    text = text.replace("${venue:singlequote}", ",".join(f"'{v}'" for v in VENUES))
    return re.sub(r"=~\"\$venue\"", '=~".*"', text)


def prometheus(base: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    targets = httpx.get(f"{base}/api/v1/targets", timeout=10).json()["data"]["activeTargets"]
    ingest = [t for t in targets if t["labels"].get("job") == "tickdb-ingest"]
    out["ingest_targets"] = len(ingest)
    out["ingest_targets_up"] = sum(t["health"] == "up" for t in ingest)
    groups = httpx.get(f"{base}/api/v1/rules", timeout=10).json()["data"]["groups"]
    rules = [r for g in groups for r in g["rules"]]
    out["rules"] = len(rules)
    out["rules_unhealthy"] = [r["name"] for r in rules if r.get("health") != "ok"]
    out["alerts_firing"] = sorted({r["name"] for r in rules if r.get("state") == "firing"})
    out["ok"] = bool(ingest) and out["ingest_targets_up"] == len(ingest) and len(rules) >= 12
    out["ok"] = out["ok"] and not out["rules_unhealthy"]
    return out


def grafana(base: str, auth: tuple[str, str]) -> dict[str, Any]:
    client = httpx.Client(base_url=base, auth=auth, timeout=60)
    found = {
        d["uid"]: d["title"] for d in client.get("/api/search", params={"tag": "tickdb"}).json()
    }
    out: dict[str, Any] = {"dashboards": found, "panels": {}, "errors": []}
    for uid in ("tickdb-feed-health", "tickdb-data-quality"):
        if uid not in found:
            out["errors"].append(f"dashboard {uid} not provisioned")
            continue
        dash = client.get(f"/api/dashboards/uid/{uid}").json()["dashboard"]
        start = dash["time"]["from"]
        for panel in dash["panels"]:
            targets = panel.get("targets") or []
            if not targets:
                continue
            queries = []
            for t in targets:
                q = json.loads(expand(json.dumps(t)))
                q.update(intervalMs=60_000, maxDataPoints=500)
                queries.append(q)
            resp = client.post(
                "/api/ds/query", json={"queries": queries, "from": start, "to": "now"}
            )
            errors = []
            if resp.status_code != 200:
                errors.append(f"HTTP {resp.status_code}: {resp.text[:200]}")
            else:
                for ref, res in resp.json().get("results", {}).items():
                    if res.get("error"):
                        errors.append(f"{ref}: {res['error'][:200]}")
            out["panels"][f"{uid}/{panel['title']}"] = errors or "ok"
            out["errors"] += [f"{uid}/{panel['title']}: {e}" for e in errors]
    yesterday = (datetime.now(UTC).date() - timedelta(days=1)).isoformat()
    sql = f"SELECT count() AS n FROM tickdb.dq_daily WHERE date = toDate('{yesterday}')"
    q = {"refId": "A", "datasource": {"type": "grafana-clickhouse-datasource", "uid": "clickhouse"},
         "editorType": "sql", "queryType": "table", "format": 1, "rawSql": sql}  # fmt: skip
    resp = client.post("/api/ds/query", json={"queries": [q], "from": "now-1h", "to": "now"})
    try:
        frame = resp.json()["results"]["A"]["frames"][0]
        n = frame["data"]["values"][0][0]
    except (KeyError, IndexError, ValueError):
        n = None
    out["dq_daily_rows_yesterday"] = n
    if not n:
        out["errors"].append(f"dq_daily has no rows for {yesterday}")
    out["ok"] = not out["errors"]
    return out


def main() -> int:
    password = os.environ.get("GRAFANA_ADMIN_PASSWORD")
    if not password:
        print("GRAFANA_ADMIN_PASSWORD is not set (run through make, which loads .env)")
        return 3
    prom_port = os.environ.get("PROMETHEUS_PORT", "9090")
    graf_port = os.environ.get("GRAFANA_PORT", "3000")
    result = {
        "checked_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "prometheus": prometheus(f"http://127.0.0.1:{prom_port}"),
        "grafana": grafana(f"http://127.0.0.1:{graf_port}", ("admin", password)),
    }
    result["ok"] = result["prometheus"]["ok"] and result["grafana"]["ok"]
    print(json.dumps(result, indent=2))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
