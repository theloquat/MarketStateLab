"""Bounded research tools. No arbitrary code, SQL, shell or automatic paid calls."""

from __future__ import annotations
import json
from datetime import date, timedelta
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from .contracts import canonical, identity, utcnow
from .features import panel
from .evaluation import evaluate, warning_metrics

TOOLS = {
    "model_compare",
    "warning_sweep",
    "quality_audit",
    "event_evidence",
    "feature_ablation",
    "macro_lag_sensitivity",
}


def create(lab, owner, spec):
    if spec.get("tool") not in TOOLS:
        raise ValueError("不支持的研究工具")
    if len(spec.get("model_ids", [])) > 4:
        raise ValueError("每次最多比较4个冻结模型")
    rid = identity([owner, spec, utcnow()])[:24]
    lab.execute(
        "INSERT INTO research VALUES(?,?,?,?,?,?,?)",
        (rid, owner, "proposed", canonical(spec), "{}", utcnow(), utcnow()),
    )
    return rid


def execute(lab, rid, actor):
    with lab.db() as c:
        c.execute("BEGIN IMMEDIATE")
        row = c.execute("SELECT * FROM research WHERE id=?", (rid,)).fetchone()
        if not row:
            raise ValueError("研究不存在")
        if row["status"] != "approved":
            raise ValueError("研究须先人工批准，不能重复执行")
        c.execute(
            "UPDATE research SET status='running',updated_at=? WHERE id=?",
            (utcnow(), rid),
        )
    spec = json.loads(row["spec"])
    try:
        tool = spec["tool"]
        ids = spec.get("model_ids", [])
        if tool == "model_compare":
            result = evaluate(lab, ids)
        elif tool == "quality_audit":
            series = lab.series()
            result = {
                "dataset": lab.dataset(),
                "coverage": series,
                "missing": [s["name"] for s in series if s["status"] != "ready"],
                "negative_preserved": sum(
                    s.get("quality", {}).get("negative_count", 0) for s in series
                ),
                "unknown_timing_sources": lab.rows("SELECT path,detail FROM sources"),
                "repair_suggestions": [
                    "缺失高收益债需另接合法来源，不插值伪造信用利差",
                    "核对未知字段与发布时间，匿名指标保留原ID",
                    "负价格、负收益率不得自动删除或取对数",
                ],
            }
        elif tool == "event_evidence":
            result = event_case(lab, spec.get("day", "2020-03-16"))
        elif tool == "macro_lag_sensitivity":
            result = macro_lag_sensitivity(lab)
        elif tool == "feature_ablation":
            result = text_ablation(lab)
        else:
            if not ids:
                raise ValueError("请选择模型")
            result = warning_sweep(lab, ids[0])
        result = {
            "research_id": rid,
            "tool": tool,
            "executed_by": actor,
            "created_at": utcnow(),
            "result": result,
            "llm_calls": 0,
        }
        lab.execute(
            "UPDATE research SET status='review',result=?,updated_at=? WHERE id=?",
            (canonical(result), utcnow(), rid),
        )
        return result
    except BaseException as exc:
        lab.execute(
            "UPDATE research SET status='failed',result=?,updated_at=? WHERE id=?",
            (canonical({"error": str(exc)}), utcnow(), rid),
        )
        raise


def event_case(lab, day):
    day = date.fromisoformat(day).isoformat()
    start = (date.fromisoformat(day) - timedelta(days=7)).isoformat()
    end = (date.fromisoformat(day) + timedelta(days=7)).isoformat()
    documents = lab.rows(
        "SELECT id,kind,day,title,source,source_row,trust FROM documents WHERE day BETWEEN ? AND ? ORDER BY day LIMIT 40",
        (start, end),
    )
    prices = []
    for s in lab.series():
        if s["status"] != "ready":
            continue
        f = pd.read_parquet(lab.root / s["path"])
        f = f[(f.observation_date >= start) & (f.observation_date <= end)]
        prices.append(
            {
                "series_id": s["id"],
                "name": s["name"],
                "kind": s["quote_kind"],
                "unit": s["unit"],
                "items": f[
                    [
                        "observation_date",
                        "available_date",
                        "value",
                        "event_id",
                        "source_row",
                    ]
                ].to_dict("records"),
            }
        )
    return {
        "event_day": day,
        "window": [start, end],
        "documents": documents,
        "observations": prices,
        "interpretation": "事件背景用于解释和弱标签，不是市场状态真值；决策只可使用当时可用的观测",
        "counterevidence": [
            "检查其他资产是否同步变化",
            "比较规则基线与统计模型",
            "检查休市、数据年龄与来源时点",
            "事件之后的材料仅作事后复盘，不倒灌模型",
        ],
    }


def warning_sweep(lab, model_id):
    from .models import load
    from .modeling import score_frame

    card, artifact = load(lab, model_id)
    f, cols, _ = panel(lab, card["dataset_id"])
    rows = [
        r
        for r in score_frame(artifact, f, cols)
        if card["train_end"] < r["day"] <= "2023-12-31"
    ]
    if not rows:
        raise ValueError("验证区间为空，训练截止需早于2023-12-31")
    result = []
    for threshold in (0.6, 0.75, 0.9):
        for cooldown in (1, 5, 10):
            last = None
            count = 0
            for row in rows:
                d = date.fromisoformat(row["day"])
                if row["probabilities"].get("高压力", 0) >= threshold and (
                    last is None or (d - last).days > cooldown
                ):
                    count += 1
                    last = d
            result.append(
                {"threshold": threshold, "cooldown": cooldown, "alerts": count}
            )
    return {
        "window": "训练截止之后至2023-12-31，仅验证区间",
        "grid": result,
        "experiments": 9,
        "claim": "仅比较预警数量与冷却效果；有效性请看目标明确的模型评价",
    }


def text_ablation(lab):
    ds = lab.dataset()["id"]
    frame, cols, _ = panel(lab, ds)
    frame.attrs = {}
    # Local deterministic extraction, no LLM/news-by-news API spending.
    news = lab.rows(
        "SELECT day,COUNT(*) count,SUM(CASE WHEN title LIKE '%风险%' OR title LIKE '%下跌%' OR title LIKE '%加息%' OR title LIKE '%危机%' OR title LIKE '%通胀%' THEN 1 ELSE 0 END) risk FROM documents WHERE dataset_id=? AND kind='news' AND day IS NOT NULL GROUP BY day ORDER BY day",
        (ds,),
    )
    if not news:
        raise ValueError("缺少新闻资料")
    n = pd.DataFrame(news)
    n["available"] = pd.to_datetime(n.day) + pd.Timedelta(days=2)
    left = pd.DataFrame({"available": pd.to_datetime(frame.day)})
    joined = pd.merge_asof(
        left,
        n[["available", "count", "risk"]],
        on="available",
        direction="backward",
        tolerance=pd.Timedelta(days=2),
    )
    data = frame.copy()
    data["news_count"] = np.log1p(joined["count"].to_numpy())
    data["risk_share"] = (joined.risk / joined["count"]).to_numpy()
    spx = next(s for s in lab.series(ds) if s["name"] == "标普500")
    prices = pd.read_parquet(lab.root / spx["path"]).reset_index(drop=True)
    labels = {}
    for _, row in data.iterrows():
        valid = prices[prices.available_date <= row.day]
        if valid.empty:
            continue
        i = int(valid.index[-1])
        future = prices.iloc[i + 1 : i + 11]
        if len(future) == 10:
            labels[row.day] = (
                int(future.value.min() / prices.iloc[i].value - 1 <= -0.05),
                future.available_date.iloc[-1],
            )
    data["target"] = [labels.get(d, (None, None))[0] for d in data.day]
    data["target_known_at"] = [labels.get(d, (None, None))[1] for d in data.day]
    data = data[
        (data.day >= "2023-09-05")
        & data[["news_count", "risk_share", "target"]].notna().all(axis=1)
    ].copy()
    train = (data.day <= "2024-12-31") & (data.target_known_at <= "2024-12-31")
    val = (data.day > "2024-12-31") & (data.day <= "2025-06-30")
    test = data.day > "2025-06-30"
    if (
        min(int(train.sum()), int(val.sum()), int(test.sum())) < 20
        or data.loc[train, "target"].nunique() < 2
    ):
        raise ValueError("同窗样本或训练目标类别不足，不能声称已完成消融")
    outcomes = {}
    for name, selected in [
        ("price_only", cols),
        ("price_and_conditional_news", cols + ["news_count", "risk_share"]),
    ]:
        scaler = StandardScaler().fit(data.loc[train, selected])
        model = LogisticRegression(
            C=0.1, class_weight="balanced", max_iter=500, random_state=17
        ).fit(
            scaler.transform(data.loc[train, selected]),
            data.loc[train, "target"].astype(int),
        )
        vp = model.predict_proba(scaler.transform(data.loc[val, selected]))[:, 1]
        candidates = [
            (
                warning_metrics(vp, data.loc[val, "target"].astype(bool), threshold=t)[
                    "f1"
                ],
                t,
            )
            for t in (0.25, 0.5, 0.75)
        ]
        threshold = max(candidates, key=lambda x: (x[0], x[1]))[1]
        probabilities = model.predict_proba(scaler.transform(data.loc[test, selected]))[
            :, 1
        ]
        outcomes[name] = {
            "threshold_selected_on_validation": threshold,
            "heldout": warning_metrics(
                probabilities,
                data.loc[test, "target"].astype(bool),
                threshold,
                days=data.loc[test, "day"].tolist(),
            ),
            "coefficients": dict(zip(selected, model.coef_[0].tolist())),
        }
    return {
        "dataset_id": ds,
        "train_end": "2024-12-31",
        "validation_end": "2025-06-30",
        "test_start": "2025-07-01",
        "same_row_mask": True,
        "train_rows": int(train.sum()),
        "validation_rows": int(val.sum()),
        "test_rows": int(test.sum()),
        "outcomes": outcomes,
        "availability_grade": "C_conditional_news_time",
        "reused_historical_test": True,
        "limitations": [
            "新闻日期+2日仅为假设，尚未核实原始字段含义",
            "新闻来源以中国资讯为主，不代表全球情绪",
            "条件性增益不证明可部署效果，不能倒灌主状态模型",
            "使用原始目标结束日期清除跨训练边界的标签",
        ],
    }


def macro_lag_sensitivity(lab):
    """Retrospective anonymous-series sensitivity, explicitly outside deployment."""
    rows = lab.rows(
        "SELECT day,title,raw_json FROM documents WHERE kind='anonymous_macro' AND title IN ('X01','X02','X03') AND day IS NOT NULL ORDER BY day"
    )
    if not rows:
        raise ValueError("缺少匿名指标记录")
    spx = next(s for s in lab.series() if s["name"] == "标普500")
    prices = pd.read_parquet(lab.root / spx["path"]).sort_values("available_date")
    target = pd.DataFrame(
        {
            "day": pd.to_datetime(prices.available_date),
            "change": prices.value.pct_change().to_numpy(),
        }
    )
    records = []
    for name in sorted({r["title"] for r in rows}):
        subset = [r for r in rows if r["title"] == name]
        keys = sorted(
            {k for r in subset for k in json.loads(r["raw_json"]) if k != "日期"}
        )[:4]
        for key in keys:
            series = (
                pd.DataFrame(
                    {
                        "day": pd.to_datetime([r["day"] for r in subset]),
                        "value": pd.to_numeric(
                            [json.loads(r["raw_json"]).get(key) for r in subset],
                            errors="coerce",
                        ),
                    }
                )
                .dropna()
                .drop_duplicates("day")
                .sort_values("day")
            )
            if len(series) < 30:
                continue
            series["delta"] = series.value.diff()
            for lag in (7, 30, 60):
                shifted = series.assign(day=series.day + pd.Timedelta(days=lag))
                joined = pd.merge_asof(
                    target,
                    shifted[["day", "delta"]],
                    on="day",
                    direction="backward",
                    tolerance=pd.Timedelta(days=31),
                ).dropna()
                corr = (
                    joined.change.corr(joined.delta)
                    if len(joined) > 30 and joined.delta.std() > 0
                    else None
                )
                records.append(
                    {
                        "series": name,
                        "field": key,
                        "assumed_lag_days": lag,
                        "matched_rows": len(joined),
                        "retrospective_same_day_correlation": float(corr)
                        if corr is not None and np.isfinite(corr)
                        else None,
                    }
                )
    return {
        "availability_grade": "C_unknown_units_release_and_revisions",
        "exploratory_only": True,
        "results": records,
        "limitations": [
            "匿名指标身份、单位、修订和发布时间未知，不映射为CPI/PMI",
            "7/30/60自然日仅做敏感性假设，不代表已获历史可得性认证",
            "回顾性相关包含多重比较，不构成预测、因果或增益证据",
            "不纳入主模型、实时预警或交易执行",
        ],
    }
