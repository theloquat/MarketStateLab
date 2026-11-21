"""Read-only batch/stream reconciliation, plus evidence checks; never uses LLMs."""

from __future__ import annotations
import argparse
import json
import os
import re
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))
from marketlab.storage import Lab  # noqa: E402
from marketlab.contracts import canonical, utcnow  # noqa: E402


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--run-id")
    p.add_argument("--output", type=Path)
    a = p.parse_args()
    # Load only this deployment’s configuration.
    for line in (ROOT / "deploy/contest/secrets.env").read_text().splitlines():
        if line and not line.startswith("#"):
            k, v = line.split("=", 1)
            os.environ.setdefault(k, v)
    os.environ.setdefault("CONTEST_MYSQL_PORT", "13306")
    os.environ.setdefault("CONTEST_REDIS_PORT", "16379")
    from marketlab.distributed import mysql, ch, cache, s3

    lab = Lab()
    rid = (
        a.run_id
        or json.loads((lab.root / "reports" / "stream-run.json").read_text())["id"]
    )
    if not re.fullmatch(r"[a-f0-9]{24}", rid):
        raise ValueError("invalid run ID")
    with mysql() as c, c.cursor() as cur:
        cur.execute("SELECT * FROM stream_runs WHERE id=%s", (rid,))
        run = cur.fetchone()
        cur.execute(
            "SELECT payload FROM stream_results WHERE run_id=%s ORDER BY day", (rid,)
        )
        stream = [json.loads(r["payload"]) for r in cur.fetchall()]
        cur.execute(
            "SELECT payload FROM stream_alerts WHERE run_id=%s ORDER BY day", (rid,)
        )
        alerts = [json.loads(r["payload"]) for r in cur.fetchall()]
        cur.execute("SELECT COUNT(*) n FROM day_features WHERE run_id=%s", (rid,))
        days = cur.fetchone()["n"]
        cur.execute("SELECT COUNT(*) n FROM outbox WHERE sent=FALSE")
        pending = cur.fetchone()["n"]
        cur.execute("SELECT COUNT(*) n FROM ch_outbox WHERE sent=FALSE")
        ch_pending = cur.fetchone()["n"]
    spec = json.loads(run["payload"])
    candidates = lab.rows("SELECT * FROM runs")
    matched = [
        r
        for r in candidates
        if all(
            json.loads(r["spec"]).get(k) == spec.get(k)
            for k in ("model_id", "start", "end", "warning_threshold", "cooldown_days")
        )
    ]
    if not matched:
        raise ValueError("create a matching completed offline reference replay first")
    local_id = matched[0]["id"]
    batch = [
        json.loads(r["payload"])
        for r in lab.rows(
            "SELECT payload FROM results WHERE run_id=? ORDER BY day", (local_id,)
        )
    ]
    local_alerts = [
        json.loads(r["payload"])
        for r in lab.rows(
            "SELECT payload FROM alerts WHERE run_id=? ORDER BY day", (local_id,)
        )
    ]
    checks = {}
    checks["same_snapshot_dates"] = [r["day"] for r in stream] == [
        r["day"] for r in batch
    ]
    checks["same_states"] = [r["state"] for r in stream] == [r["state"] for r in batch]
    checks["same_warning_dates"] = [r["result"]["day"] for r in alerts] == [
        r["result"]["day"] for r in local_alerts
    ]
    feature_error = 0.0
    prob_error = 0.0
    for left, right in zip(batch, stream):
        feature_error = max(
            feature_error,
            max(
                abs(left["features"][k] - right["features"][k])
                for k in left["features"]
            ),
        )
        prob_error = max(prob_error, abs(left["probability"] - right["probability"]))
    checks["feature_max_abs_error_lt_1e-8"] = feature_error < 1e-8
    checks["probability_max_abs_error_lt_1e-8"] = prob_error < 1e-8
    checks["all_days_materialized"] = days == spec.get("expected_days")
    checks["outbox_drained"] = pending == 0
    checks["clickhouse_outbox_drained"] = ch_pending == 0
    checks["all_real_observations_sent"] = run["sent"] == spec.get("input_records")
    counts = {
        table: json.loads(
            ch(f"SELECT count() n FROM {table} FINAL WHERE run_id='{rid}' FORMAT JSON")
        )["data"][0]["n"]
        for table in ("observations", "features", "states", "alerts")
    }
    checks["clickhouse_input_count"] = int(counts["observations"]) == run["sent"]
    checks["clickhouse_state_count"] = int(counts["states"]) == len(batch)
    checks["clickhouse_feature_count"] = int(counts["features"]) == run["sent"]
    checks["clickhouse_warning_count"] = int(counts["alerts"]) == len(local_alerts)
    checks["redis_has_last_state"] = json.loads(
        cache().get("quantos:state:" + rid) or "{}"
    ).get("day") == (batch[-1]["day"] if batch else None)
    objects = list(s3().list_objects("quantos", prefix="checkpoints/", recursive=True))
    checks["minio_checkpoint_metadata_exists"] = any(
        o.object_name.endswith("_metadata") for o in objects
    )
    report = {
        "checked_at": utcnow(),
        "run_id": rid,
        "reference_run_id": local_id,
        "passed": all(checks.values()),
        "checks": checks,
        "input_records": run["sent"],
        "daily_windows": days,
        "offline_snapshots": len(batch),
        "flink_snapshots": len(stream),
        "offline_alerts": len(local_alerts),
        "flink_alerts": len(alerts),
        "max_feature_error": feature_error,
        "max_probability_error": prob_error,
        "clickhouse_final_counts": counts,
        "performance_targets": "not measured; these are correctness checks, not throughput claims",
        "llm_calls": 0,
    }
    out = a.output or lab.root / "reports" / "batch-stream-verification.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(canonical(report))
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
