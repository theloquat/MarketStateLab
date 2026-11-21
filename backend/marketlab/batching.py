"""Causal transitions and bounded SQL microbatches; no per-day database round trips."""

from __future__ import annotations
import orjson as json
import time
from datetime import date
from itertools import groupby
from .payloads import wire
from .models import infer


def transition(spec, card, artifact, latest, alpha, batch):
    latest = dict(latest)
    day = batch["day"]
    changed = set()
    for feature in batch["features"]:
        latest[feature["series_id"]] = feature
        changed.add(feature["series_id"])
    core = [latest.get(sid) for sid in spec["core_ids"]]
    valid = all(
        f
        and f.get("momentum20") is not None
        and 0
        <= (date.fromisoformat(day) - date.fromisoformat(f["available_date"])).days
        <= 7
        for f in core
    )
    result = None
    new_alpha = alpha
    if (
        not batch.get("scored_in_flink")
        and valid
        and changed.intersection(spec["core_ids"])
        and "2020-01-01" <= day <= spec["end"]
        and (artifact.get("method") == "hmm" or day >= spec["start"])
    ):
        values = [
            latest[column.rsplit(":", 1)[0]][column.rsplit(":", 1)[1]]
            for column in artifact["columns"]
        ]
        started = time.perf_counter_ns()
        prediction = infer(artifact, values, alpha)
        inference_ms = (time.perf_counter_ns() - started) / 1e6
        new_alpha = prediction.get("filtering_state", alpha)
        if day >= spec["start"]:
            result = {
                **prediction,
                "day": day,
                "run_id": spec["id"],
                "model_id": spec["model_id"],
                "dataset_id": spec["dataset_id"],
                "evaluation": (
                    "in_sample"
                    if day <= card["train_end"]
                    else "frozen_forward_scoring"
                ),
                "clock": "historical_reconstructed",
                "engine": "kafka-flink-v1",
                "timing": {
                    "window_triggered_at_ns": batch.get("window_triggered_at_ns"),
                    "features_completed_at_ns": batch.get("features_completed_at_ns"),
                    "inference_ms": inference_ms,
                    "inference_finished_at_ns": time.time_ns(),
                    "feature_received_at_ns": batch.get("_received_at_ns"),
                },
                "features": dict(zip(artifact["columns"], values)),
                "evidence": [
                    {
                        k: f[k]
                        for k in (
                            "series_id",
                            "event_id",
                            "observation_date",
                            "available_date",
                            "source_row",
                            "value",
                        )
                    }
                    for f in core
                ],
            }
    return (
        latest,
        new_alpha,
        result,
        not batch.get("scored_in_flink")
        and valid
        and changed.intersection(spec["core_ids"])
        and "2020-01-01" <= day <= spec["end"],
    )


def feature_group(batches, receipt=None):
    from .distributed import mysql, object_model, TOPICS
    from .contracts import identity

    if not batches:
        return
    rid = batches[0]["stream_run_id"]
    if len(batches) > 64 or any(b["stream_run_id"] != rid for b in batches):
        raise ValueError(
            "Feature microbatch must contain at most 64 ordered windows of one run"
        )
    with mysql() as connection, connection.cursor() as cur:
        cur.execute("SELECT * FROM stream_runs WHERE id=%s FOR UPDATE", (rid,))
        run = cur.fetchone()
        if not run:
            raise ValueError("unknown stream run")
        spec = json.loads(run["payload"])
        cur.execute(
            "SELECT payload FROM day_features WHERE run_id=%s ORDER BY day DESC LIMIT 1",
            (rid,),
        )
        previous = cur.fetchone()
        previous = json.loads(previous["payload"]) if previous else None
        latest = {f["series_id"]: f for f in previous["latest"]} if previous else {}
        last_day = previous["day"] if previous else ""
        cur.execute(
            "SELECT day FROM day_features WHERE run_id=%s AND day BETWEEN %s AND %s",
            (rid, min(b["day"] for b in batches), max(b["day"] for b in batches)),
        )
        existing = {r["day"] for r in cur.fetchall()}
        cur.execute(
            "SELECT payload FROM lab_meta WHERE id=%s", ("model:" + spec["model_id"],)
        )
        card = json.loads(cur.fetchone()["payload"])
        artifact = object_model(spec["model_id"], card["artifact_hash"])
        cur.execute(
            "SELECT payload FROM filter_states WHERE run_id=%s ORDER BY day DESC LIMIT 1",
            (rid,),
        )
        stored = cur.fetchone()
        alpha = json.loads(stored["payload"]) if stored else None
        features = []
        filters = []
        results = []
        events = []
        deliveries = []
        observations = 0
        for batch in batches:
            day = batch["day"]
            if day in existing:
                continue
            if day <= last_day:
                raise ValueError(
                    "feature day moved backwards; explicit new replay required"
                )
            latest, alpha, result, valid = transition(
                spec, card, artifact, latest, alpha, batch
            )
            features.append(
                (rid, day, wire({**batch, "latest": list(latest.values())}))
            )
            if valid and artifact.get("method") == "hmm":
                filters.append((rid, day, wire(alpha)))
            if result:
                payload = wire(result)
                results.append((rid, day, payload))
                events.append(("state:" + rid + ":" + day, TOPICS[2], payload))
            facts = [
                {
                    "id": f["event_id"],
                    "run_id": rid,
                    "day": day,
                    "payload": wire(f),
                }
                for f in batch["features"]
            ]
            key = "features:batch:" + identity([(r["run_id"], r["id"]) for r in facts])
            deliveries.append((key, "features", wire({"batch": facts})))
            observations += len(batch["features"])
            existing.add(day)
            last_day = day
        for sql, rows in [
            ("INSERT INTO day_features VALUES(%s,%s,%s)", features),
            ("INSERT INTO filter_states VALUES(%s,%s,%s)", filters),
            ("INSERT INTO stream_results VALUES(%s,%s,%s)", results),
            ("INSERT IGNORE INTO outbox(id,topic,payload) VALUES(%s,%s,%s)", events),
            (
                "INSERT IGNORE INTO ch_outbox(id,table_name,payload) VALUES(%s,%s,%s)",
                deliveries,
            ),
        ]:
            if rows:
                cur.executemany(sql, rows)
        cur.execute(
            "UPDATE stream_runs SET processed=processed+%s WHERE id=%s",
            (observations, rid),
        )
        cur.execute("SELECT COUNT(*) n FROM day_features WHERE run_id=%s", (rid,))
        if cur.fetchone()["n"] == spec.get("expected_days"):
            cur.execute(
                "UPDATE stream_runs SET status='features_completed' WHERE id=%s", (rid,)
            )
        if receipt:
            from .distributed import record_offset

            record_offset(cur, receipt)
        connection.commit()


def feature_messages(messages, role="features"):
    for message in messages:
        message.value["_received_at_ns"] = time.time_ns()
    for _, group in groupby(messages, key=lambda m: m.value["stream_run_id"]):
        items = list(group)
        for index in range(0, len(items), 16):
            chunk = items[index : index + 16]
            last = chunk[-1]
            feature_group([m.value for m in chunk], (role, last.partition, last.offset))


def warning_payloads(spec, results, previous_day=None):
    from .contracts import identity

    output = []
    for result in results:
        day = result["day"]
        if (
            result["state"] == "高压力"
            and result["probability"] >= spec["warning_threshold"]
            and (
                previous_day is None
                or (date.fromisoformat(day) - date.fromisoformat(previous_day)).days
                > spec["cooldown_days"]
            )
        ):
            aid = identity([spec["id"], day, "high-pressure-v1"])[:24]
            output.append(
                {
                    "id": aid,
                    "rule": "high-pressure-v1",
                    "threshold": spec["warning_threshold"],
                    "cooldown_days": spec["cooldown_days"],
                    "result": result,
                }
            )
            previous_day = day
    return output


def state_group(results, receipt=None):
    from .distributed import mysql, record_offset, enqueue_ch, TOPICS
    from .contracts import identity

    if not results:
        return
    rid = results[0]["run_id"]
    if len(results) > 64 or any(r["run_id"] != rid for r in results):
        raise ValueError(
            "State microbatch must contain at most 64 ordered states of one run"
        )
    with mysql() as connection, connection.cursor() as cur:
        cur.execute("SELECT payload FROM stream_runs WHERE id=%s", (rid,))
        row = cur.fetchone()
        if not row:
            raise ValueError("Unknown stream run")
        spec = json.loads(row["payload"])
        cur.execute(
            "SELECT day FROM state_receipts WHERE run_id=%s AND day BETWEEN %s AND %s",
            (rid, min(r["day"] for r in results), max(r["day"] for r in results)),
        )
        seen = {r["day"] for r in cur.fetchall()}
        cur.execute(
            "SELECT day FROM state_receipts WHERE run_id=%s ORDER BY day DESC LIMIT 1",
            (rid,),
        )
        last = cur.fetchone()
        last_day = last["day"] if last else ""
        fresh = []
        for result in results:
            day = result["day"]
            if day in seen:
                continue
            if day <= last_day:
                raise ValueError(
                    "State day moved backwards; explicit new replay required"
                )
            fresh.append(result)
            seen.add(day)
            last_day = day
        cur.execute(
            "SELECT day FROM stream_alerts WHERE run_id=%s ORDER BY day DESC LIMIT 1",
            (rid,),
        )
        previous = cur.fetchone()
        warnings = warning_payloads(spec, fresh, previous["day"] if previous else None)
        receipts = []
        persisted = []
        filters = []
        events = []
        states = []
        alerts = []
        alert_facts = []
        for result in fresh:
            result.setdefault("timing", {})["state_prepared_at_ns"] = time.time_ns()
            day = result["day"]
            payload = wire(result)
            receipts.append((rid, day))
            states.append(
                {
                    "id": identity([rid, day]),
                    "run_id": rid,
                    "day": day,
                    "payload": payload,
                }
            )
            if result.get("inference_engine") == "flink-compiled-v1":
                persisted.append((rid, day, payload))
                events.append(
                    ("persisted-state:" + rid + ":" + day, TOPICS[5], payload)
                )
                if result.get("filtering_state") is not None:
                    filters.append((rid, day, wire(result["filtering_state"])))
        for alert in warnings:
            day = alert["result"]["day"]
            payload = wire(alert)
            alerts.append((alert["id"], rid, day, payload))
            events.append(("alert:" + alert["id"], TOPICS[3], payload))
            alert_facts.append(
                {"id": alert["id"], "run_id": rid, "day": day, "payload": payload}
            )
        for sql, rows in [
            ("INSERT INTO state_receipts VALUES(%s,%s)", receipts),
            ("INSERT IGNORE INTO stream_results VALUES(%s,%s,%s)", persisted),
            ("INSERT IGNORE INTO filter_states VALUES(%s,%s,%s)", filters),
            (
                "INSERT IGNORE INTO stream_alerts(id,run_id,day,payload) VALUES(%s,%s,%s,%s)",
                alerts,
            ),
            ("INSERT IGNORE INTO outbox(id,topic,payload) VALUES(%s,%s,%s)", events),
        ]:
            if rows:
                cur.executemany(sql, rows)
        enqueue_ch(cur, "states", states)
        enqueue_ch(cur, "alerts", alert_facts)
        record_offset(cur, receipt)
        connection.commit()


def state_messages(messages, role="alerts"):
    for _, group in groupby(messages, key=lambda m: m.value["run_id"]):
        items = list(group)
        for index in range(0, len(items), 64):
            chunk = items[index : index + 64]
            last = chunk[-1]
            received = time.time_ns()
            values = [
                {
                    **m.value,
                    "timing": {
                        **m.value.get("timing", {}),
                        "state_received_at_ns": received,
                    },
                }
                for m in chunk
            ]
            state_group(values, (role, last.partition, last.offset))
