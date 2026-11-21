"""Pre-specified temporal evaluation; labels are future stress targets, not regime truth."""

from __future__ import annotations
import numpy as np
import pandas as pd
from .contracts import canonical, identity, utcnow
from .features import panel
from .models import load
from .modeling import score_frame


def warning_metrics(probabilities, target, threshold=0.75, cooldown=5, days=None):
    p = np.asarray(probabilities, float)
    truth = np.asarray(target, bool)
    if (
        p.ndim != 1
        or truth.shape != p.shape
        or not np.isfinite(p).all()
        or np.any((p < 0) | (p > 1))
    ):
        raise ValueError("概率与标签必须是等长的一维有限概率")
    if days is not None and (len(days) != len(p) or list(days) != sorted(days)):
        raise ValueError("评价日期必须等长且按时间排列")
    predicted = p >= threshold
    tp = int(np.sum(predicted & truth))
    fp = int(np.sum(predicted & ~truth))
    fn = int(np.sum(~predicted & truth))
    precision = tp / max(tp + fp, 1)
    recall = tp / max(tp + fn, 1)
    alerts = []
    last = -100000
    for i, flag in enumerate(predicted):
        t = pd.Timestamp(days[i]).toordinal() if days is not None else i
        if flag and t - last > cooldown:
            alerts.append(i)
            last = t
    episodes = []
    start = None
    for i, flag in enumerate(truth):
        if flag and start is None:
            start = i
        if start is not None and (not flag or i == len(truth) - 1):
            episodes.append((start, i if flag else i - 1))
            start = None
    return {
        "evaluated_snapshots": len(p),
        "precision": precision,
        "recall": recall,
        "f1": 2 * precision * recall / max(precision + recall, 1e-12),
        "true_positive_days": tp,
        "false_positive_days": fp,
        "false_negative_days": fn,
        "false_positive_days_per_100": 100 * fp / max(len(p), 1),
        "cooldown_alerts": len(alerts),
        "stress_episodes": len(episodes),
        "episode_recall": sum(
            any(start <= i <= end for i in alerts) for start, end in episodes
        )
        / max(len(episodes), 1),
        "target": "next 10 observed SPX closes drawdown >=5%; overlapping daily labels coalesced into episodes",
        "caveat": "episode recall is overlap coverage, not independently validated early-warning lead time",
    }


def evaluate(lab, model_ids, validation_end="2023-12-31", test_start="2024-01-01"):
    if not model_ids:
        raise ValueError("请选择冻结模型")
    cards = [load(lab, m)[0] for m in model_ids]
    if len({c["dataset_id"] for c in cards}) != 1:
        raise ValueError("比较必须使用相同数据版本")
    if max(c["train_end"] for c in cards) >= test_start:
        raise ValueError("留出区间必须晚于所有模型训练截止")
    if validation_end >= test_start:
        raise ValueError("验证和测试区间重叠")
    ds = cards[0]["dataset_id"]
    frame, columns, _ = panel(lab, ds)
    frame.attrs = {}
    spx = next(s for s in lab.series(ds) if s["name"] == "标普500")
    prices = (
        pd.read_parquet(lab.root / spx["path"])
        .sort_values("available_date")
        .reset_index(drop=True)
    )
    targets = {}
    target_known_at = {}
    for _, r in frame.iterrows():
        eligible = prices[prices.available_date <= r.day]
        if eligible.empty:
            continue
        i = int(eligible.index[-1])
        future = prices.iloc[i + 1 : i + 11]
        if len(future) == 10:
            target_known_at[r.day] = future.available_date.iloc[-1]
            targets[r.day] = bool(
                future.value.min() / prices.iloc[i].value - 1 <= -0.05
            )
    spx_momentum = next(
        c
        for c, m in zip(columns, cards[0]["features"])
        if m["name"] == "标普500" and m["measure"] == "momentum20"
    )
    spx_vol = next(
        c
        for c, m in zip(columns, cards[0]["features"])
        if m["name"] == "标普500" and m["measure"] == "vol20"
    )
    all_results = {}
    for card in cards:
        _, artifact = load(lab, card["id"])
        rows = score_frame(artifact, frame, columns)
        stages = {}
        for stage, mask in [
            ("validation", lambda d: card["train_end"] < d <= validation_end),
            ("heldout", lambda d: d >= test_start),
        ]:
            selected = [
                r
                for r in rows
                if mask(r["day"])
                and r["day"] in targets
                and (
                    stage != "validation" or target_known_at[r["day"]] <= validation_end
                )
            ]
            if not selected:
                continue
            probs = [r["probabilities"].get("高压力", 0) for r in selected]
            truth = [targets[r["day"]] for r in selected]
            states = [r["state"] for r in selected]
            switches = sum(a != b for a, b in zip(states, states[1:]))
            occupancy = {s: states.count(s) / len(states) for s in sorted(set(states))}
            state_profiles = {}
            indexed = frame.set_index("day")
            current_flags = []
            for r in selected:
                snapshot = indexed.loc[r["day"]]
                current_flags.append(
                    bool(snapshot[spx_momentum] <= -0.05 or snapshot[spx_vol] >= 0.02)
                )
            for state in sorted(set(states)):
                state_days = [r["day"] for r in selected if r["state"] == state]
                subset = indexed.loc[state_days]
                state_profiles[state] = {
                    "snapshots": len(subset),
                    "median_spx_momentum20": float(subset[spx_momentum].median()),
                    "median_spx_vol20": float(subset[spx_vol].median()),
                }
            stages[stage] = {
                **warning_metrics(probs, truth, days=[r["day"] for r in selected]),
                "contemporaneous_rule_agreement": {
                    **warning_metrics(
                        probs, current_flags, days=[r["day"] for r in selected]
                    ),
                    "target": "same-time fixed SPX momentum/vol pressure proxy; uses overlapping model inputs; descriptive agreement, not independent truth",
                },
                "state_profiles": state_profiles,
                "start": selected[0]["day"],
                "end": selected[-1]["day"],
                "state_switch_rate": switches / max(len(states) - 1, 1),
                "state_occupancy": occupancy,
                "mean_log_evidence": float(
                    np.mean([r["log_evidence"] for r in selected])
                ),
                "dimensions": len(artifact["means"][0]),
                "log_evidence_comparable_only_same_transform": True,
            }
        all_results[card["id"]] = {
            "method": card["method"],
            "components": len(artifact["means"]),
            "reduction": card.get("reduction", "none"),
            "stages": stages,
        }
    # Transparent, fixed price rule on identical eligible heldout snapshots.
    spx_features = [
        (c, m) for c, m in zip(columns, card["features"]) if m["name"] == "标普500"
    ]
    baseline_rows = frame[(frame.day >= test_start) & frame.day.isin(targets)]
    fixed_rule = np.zeros(len(baseline_rows), dtype=bool)
    for column, meta in spx_features:
        if meta["measure"] == "momentum20":
            fixed_rule |= baseline_rows[column].to_numpy() <= -0.05
        if meta["measure"] == "vol20":
            fixed_rule |= baseline_rows[column].to_numpy() >= 0.02
    baseline = warning_metrics(
        fixed_rule.astype(float),
        [targets[d] for d in baseline_rows.day],
        days=baseline_rows.day.tolist(),
    )
    baseline["definition"] = (
        "SPX 20-observed-transition momentum <= -5% OR daily vol >= 2%; fixed before comparison, no tuning"
    )
    scope = identity([ds, test_start, "SPX_future10_dd5_v1"])[:24]
    with lab.db() as c:
        c.execute(
            "CREATE TABLE IF NOT EXISTS test_exposures(scope TEXT PRIMARY KEY,count INTEGER)"
        )
        c.execute(
            "INSERT INTO test_exposures VALUES(?,1) ON CONFLICT(scope) DO UPDATE SET count=count+1",
            (scope,),
        )
        exposures = c.execute(
            "SELECT count FROM test_exposures WHERE scope=?", (scope,)
        ).fetchone()[0]
    prior_runs = lab.rows(
        "SELECT COUNT(*) n FROM runs WHERE json_extract(spec,'$.dataset_id')=? AND json_extract(spec,'$.end')>=? AND cursor>0",
        (ds, test_start),
    )[0]["n"]
    result = {
        "dataset_id": ds,
        "models": all_results,
        "fixed_price_rule_heldout": baseline,
        "validation_end": validation_end,
        "test_start": test_start,
        "exposure_count": exposures,
        "prior_inspected_runs": prior_runs,
        "reused_holdout": exposures > 1 or prior_runs > 0,
        "policy": "parameters frozen before validation/heldout evaluation; no accuracy target for unsupervised regimes",
        "limitations": [
            "历史数据为重建可得性，非严格PIT",
            "匿名宏观/事后推文不进入主模型",
            "不同PCA维度的似然不可直接排名",
            "预警目标不是状态真值或盈利证明",
        ],
        "created_at": utcnow(),
    }
    rid = identity([ds, model_ids, validation_end, test_start])[:24]
    result["id"] = rid
    path = lab.root / "reports" / ("evaluation-" + rid + ".json")
    path.write_text(canonical(result))
    lab.execute(
        "INSERT OR REPLACE INTO analyses VALUES(?,?,?)",
        (rid, "model_evaluation", canonical(result)),
    )
    return result
