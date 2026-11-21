"""Causal per-series reference, shared specification with the Flink job.

20 observed transitions, not 20 universal trading days. Cross-market joins use
reconstructed available dates and expire after 7 calendar days.
"""

from __future__ import annotations

import pandas as pd

from .contracts import FEATURE_VERSION

CORE_NAMES = (
    "标普500",
    "沪深300",
    "纳斯达克100",
    "日经225",
    "COMEX黄金期货",
    "Brent原油",
    "美元指数DXY",
    "美国10年期国债收益率",
)


def per_series(df):
    df = df.sort_values("observation_date").copy()
    prev = df.value.shift(1)
    if df.quote_kind.iloc[0] == "yield":
        change = (df.value - prev) * 100.0
    else:
        change = (df.value / prev - 1.0).where((df.value > 0) & (prev > 0))
    df["change1"] = change
    df["mean20"] = change.rolling(20, min_periods=20).mean()
    df["vol20"] = change.rolling(20, min_periods=20).std(ddof=0)
    df["momentum20"] = change.rolling(20, min_periods=20).sum()
    df["feature_version"] = FEATURE_VERSION
    return df


def panel(lab, dataset_id, *, until=None):
    series = {s["name"]: s for s in lab.series(dataset_id) if s["status"] == "ready"}
    missing = [n for n in CORE_NAMES if n not in series]
    if missing:
        raise ValueError(f"缺少固定核心资产: {missing}")
    frames, columns, metadata = [], [], []
    for n in CORE_NAMES:
        s = series[n]
        df = pd.read_parquet(lab.root / s["path"])
        if until:
            df = df[df.available_date <= until]
        if df.empty:
            raise ValueError(f"训练截止前无可用观测: {n}")
        f = per_series(df)
        frames.append((s, f))
        for col in ("momentum20", "vol20"):
            key = s["id"] + ":" + col
            columns.append(key)
            metadata.append(
                {
                    "key": key,
                    "series_id": s["id"],
                    "name": s["name"],
                    "measure": col,
                    "unit": "bps" if s["quote_kind"] == "yield" else "fraction",
                    "definition": "最近20个有效观测变化之和"
                    if col == "momentum20"
                    else "最近20个有效观测变化总体标准差",
                    "window": "20 observed transitions; invalid transition breaks window",
                }
            )
    first = max(f.available_date.min() for _, f in frames)
    last = max(f.available_date.max() for _, f in frames)
    days = pd.DataFrame(
        {
            "day": pd.date_range(max(first, "2020-01-01"), last, freq="D").strftime(
                "%Y-%m-%d"
            )
        }
    )
    evidence = {}
    for s, f in frames:
        right = f[
            [
                "available_date",
                "observation_date",
                "event_id",
                "source_row",
                "value",
                "momentum20",
                "vol20",
            ]
        ].copy()
        right["available_ts"] = pd.to_datetime(right.available_date)
        joined = pd.merge_asof(
            pd.DataFrame({"ts": pd.to_datetime(days.day)}),
            right,
            left_on="ts",
            right_on="available_ts",
            direction="backward",
            tolerance=pd.Timedelta(days=7),
        )
        for col in ("momentum20", "vol20"):
            days[s["id"] + ":" + col] = joined[col].values
        evidence[s["id"]] = joined
    mask = days[columns].notna().all(axis=1)
    # Only infer when at least one source advances, never manufacture weekend updates.
    fingerprint = pd.DataFrame(
        {sid: frame.event_id for sid, frame in evidence.items()}
    ).fillna("")
    advances = (fingerprint != fingerprint.shift(1)).any(axis=1)
    idx = days.index[mask & advances]
    result = days.loc[idx].reset_index(drop=True)
    result.attrs["evidence"] = {
        str(days.loc[i, "day"]): [
            {
                "series_id": sid,
                "event_id": frame.loc[i, "event_id"],
                "observation_date": frame.loc[i, "observation_date"],
                "available_date": frame.loc[i, "available_date"],
                "source_row": int(frame.loc[i, "source_row"]),
                "value": float(frame.loc[i, "value"]),
                "age_days": (
                    pd.Timestamp(days.loc[i, "day"]) - frame.loc[i, "available_ts"]
                ).days,
            }
            for sid, frame in evidence.items()
        ]
        for i in idx
    }
    return result, columns, metadata
