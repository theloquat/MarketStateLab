from __future__ import annotations

import json
from datetime import date

from .contracts import ReplayRequest, canonical, identity, utcnow
from .features import panel
from .models import infer, load


def create_run(lab, request: ReplayRequest):
    card, _ = load(lab, request.model_id)
    frame, _, _ = panel(lab, card["dataset_id"])
    frame = frame[
        (frame.day >= request.start.isoformat())
        & (frame.day <= request.end.isoformat())
    ]
    if frame.empty:
        raise ValueError("所选区间没有满足覆盖和窗口条件的特征快照")
    spec = {
        **request.model_dump(mode="json"),
        "dataset_id": card["dataset_id"],
        "engine": "local-reference-v1",
    }
    rid = identity(spec)[:24]
    lab.execute(
        "INSERT OR IGNORE INTO runs(id,spec,status,cursor,total,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
        (rid, canonical(spec), "paused", 0, len(frame), utcnow(), utcnow()),
    )
    return rid


def advance(lab, run_id, steps=1):
    # SQLite write transaction serializes concurrent advances: result, warning and cursor commit together.
    run = lab.rows("SELECT * FROM runs WHERE id=?", (run_id,))
    if not run:
        raise ValueError("回放不存在")
    spec = json.loads(run[0]["spec"])
    card, artifact = load(lab, spec["model_id"])
    frame, columns, _ = panel(lab, spec["dataset_id"])
    evidence = frame.attrs["evidence"]
    scored = None
    if artifact.get("method") == "hmm":
        from .modeling import score_frame

        scored = {r["day"]: r for r in score_frame(artifact, frame, columns)}
    frame = frame[
        (frame.day >= spec["start"]) & (frame.day <= spec["end"])
    ].reset_index(drop=True)
    with lab.db() as c:
        c.execute("BEGIN IMMEDIATE")
        current = c.execute("SELECT * FROM runs WHERE id=?", (run_id,)).fetchone()
        cursor = current["cursor"]
        stop = min(cursor + steps, len(frame))
        last_alert = c.execute(
            "SELECT day FROM alerts WHERE run_id=? ORDER BY day DESC LIMIT 1", (run_id,)
        ).fetchone()
        last_day = last_alert["day"] if last_alert else None
        for i in range(cursor, stop):
            row = frame.iloc[i]
            result = {
                **(
                    scored[row.day]
                    if scored is not None
                    else infer(artifact, row[columns].to_numpy(dtype=float))
                ),
                "day": row.day,
                "model_id": spec["model_id"],
                "dataset_id": spec["dataset_id"],
                "evaluation": "in_sample"
                if row.day <= card["train_end"]
                else "frozen_forward_scoring",
                "features": {col: float(row[col]) for col in columns},
                "evidence": evidence[row.day],
                "clock": "historical_reconstructed",
                "run_id": run_id,
            }
            c.execute(
                "INSERT OR IGNORE INTO results VALUES(?,?,?)",
                (run_id, row.day, canonical(result)),
            )
            if (
                result["state"] == "高压力"
                and result["probability"] >= spec["warning_threshold"]
                and (
                    last_day is None
                    or (date.fromisoformat(row.day) - date.fromisoformat(last_day)).days
                    > spec["cooldown_days"]
                )
            ):
                aid = identity([run_id, row.day, "high-pressure-v1"])[:24]
                payload = {
                    "id": aid,
                    "rule": "high-pressure-v1",
                    "threshold": spec["warning_threshold"],
                    "cooldown_days": spec["cooldown_days"],
                    "result": result,
                }
                c.execute(
                    "INSERT OR IGNORE INTO alerts(id,run_id,day,payload) VALUES(?,?,?,?)",
                    (aid, run_id, row.day, canonical(payload)),
                )
                last_day = row.day
        c.execute(
            "UPDATE runs SET cursor=?,status=?,updated_at=? WHERE id=?",
            (stop, "completed" if stop == len(frame) else "paused", utcnow(), run_id),
        )
    return lab.rows("SELECT * FROM runs WHERE id=?", (run_id,))[0]
